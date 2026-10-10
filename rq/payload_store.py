"""Content-addressed store for offloaded job payloads.

Large job payloads (the exact bytes kept in a job hash's ``data`` field) can be moved
to this store. A job then only holds a small reference marker::

    rq-payload:v1:sha256:<hexdigest>

Payloads are content-addressed by their SHA-256 digest, so identical payloads are stored
once and shared by any number of jobs. The store never modifies job hashes itself; the
only callers that swap a job's ``data`` field are the migrator (:mod:`rq.payload_migration`)
and ordinary RQ write paths.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from .exceptions import PayloadIntegrityError, PayloadNotFoundError
from .utils import as_text

if TYPE_CHECKING:
    from redis import Redis

logger = logging.getLogger('rq.payload_store')

#: Versioned prefix of a job's offloaded-payload marker, stored verbatim in the
#: job hash's ``data`` field.
PAYLOAD_MARKER_PREFIX = b'rq-payload:v1:'

#: Hash algorithms allowed in markers.
SUPPORTED_DIGEST_ALGORITHMS = frozenset({'sha256'})

#: Namespace holding blob string keys: ``rq:payload:blob:<algo>:<digest>``.
BLOB_KEY_PREFIX = 'rq:payload:blob:'

#: SET indexing every blob token (``<algo>:<digest>``) known to the store.
BLOB_INDEX_KEY = 'rq:payload:blobs'

# Exact shape: rq-payload:v1:<algo>:<hex digest>
_MARKER_RE = re.compile(rb'^rq-payload:v1:([a-z0-9]+):([0-9a-f]+)$')

# A job hash key: rq:job:<job id>. Excludes auxiliary keys such as
# rq:job:<id>:dependents and rq:job::<id>:dependencies. Job IDs match
# JOB_ID_PATTERN in rq.job; duplicated here to avoid a circular import.
_JOB_KEY_RE = re.compile(r'^rq:job:[A-Za-z0-9_-]+$')


@dataclass(frozen=True)
class PayloadRef:
    """A reference to a payload stored in the content store.

    Attributes:
        algorithm: Hash algorithm, currently always ``sha256``.
        digest: Hex digest of the payload bytes.
    """

    algorithm: str
    digest: str

    @property
    def token(self) -> str:
        """Compact ``<algorithm>:<digest>`` representation used in the index SET."""
        return f'{self.algorithm}:{self.digest}'

    @property
    def blob_key(self) -> str:
        """Redis key holding the payload bytes."""
        return BLOB_KEY_PREFIX + self.token

    def to_marker(self) -> bytes:
        """The bytes written into a job hash's ``data`` field."""
        return PAYLOAD_MARKER_PREFIX + self.token.encode('ascii')

    @classmethod
    def from_marker(cls, marker: bytes) -> PayloadRef:
        """Parse a marker read from a job hash.

        Args:
            marker: The raw ``data`` field value.

        Raises:
            PayloadIntegrityError: If the marker is malformed, uses an unsupported
                algorithm, or a digest of the wrong shape.
        """
        match = _MARKER_RE.match(marker)
        if not match:
            raise PayloadIntegrityError(f'Malformed payload reference: {marker[:40]!r}')
        algorithm = match.group(1).decode('ascii')
        digest = match.group(2).decode('ascii')
        if algorithm not in SUPPORTED_DIGEST_ALGORITHMS:
            raise PayloadIntegrityError(f'Unsupported payload digest algorithm: {algorithm}')
        expected_length = len(hashlib.new(algorithm).hexdigest())
        if len(digest) != expected_length:
            raise PayloadIntegrityError(
                f'Bad {algorithm} digest length in payload reference: {len(digest)} != {expected_length}'
            )
        return cls(algorithm=algorithm, digest=digest)


def is_payload_marker(data: bytes) -> bool:
    """Whether a raw job ``data`` field value is an offloaded-payload marker."""
    return isinstance(data, bytes | bytearray) and data.startswith(PAYLOAD_MARKER_PREFIX)


def ref_for_payload(data: bytes, algorithm: str = 'sha256') -> PayloadRef:
    """Compute the content-addressed reference for payload bytes."""
    digest = hashlib.new(algorithm, data).hexdigest()
    return PayloadRef(algorithm=algorithm, digest=digest)


def is_job_key(key: bytes | str) -> bool:
    """Whether a Redis key is a job hash key (``rq:job:<job_id>``), as opposed to
    auxiliary keys like ``rq:job:<id>:dependents`` / ``rq:job::<id>:dependencies``.
    """
    return bool(_JOB_KEY_RE.match(as_text(key)))


def scan_job_keys(connection: Redis, batch_size: int = 500) -> Iterator[bytes]:
    """Yield the Redis keys of job hashes (``rq:job:<job_id>``) via SCAN.

    Auxiliary keys (``:dependents``, ``::dependencies``) are filtered out.
    SCAN is at-least-once; callers must make per-key work idempotent.
    """
    cursor: int = 0
    while True:
        next_cursor, keys = connection.scan(cursor=cursor, match='rq:job:*', count=batch_size)
        for key in keys or ():
            if is_job_key(key):
                yield cast(bytes, key)
        cursor = int(next_cursor)
        if cursor == 0:
            break


@dataclass
class PayloadGCReport:
    """Result of a garbage collection sweep."""

    #: Number of job hashes scanned for live references.
    scanned_jobs: int = 0
    #: Number of distinct blobs referenced by live jobs.
    live_blobs: int = 0
    #: Number of blob keys in the index before the sweep.
    indexed_blobs: int = 0
    #: Number of unreferenced blob keys deleted by the sweep.
    removed_blobs: int = 0


class PayloadStore:
    """Content-addressed blob store backed by Redis.

    Blobs live at :attr:`PayloadRef.blob_key`; a SET at :data:`BLOB_INDEX_KEY`
    tracks every stored token so garbage collection can enumerate blobs without
    relying on keyspace scans.
    """

    def __init__(self, connection: Redis):
        self.connection = connection

    def put(self, data: bytes) -> PayloadRef:
        """Store payload bytes once and return their reference.

        Identical bytes always resolve to the same key (``SET NX``); the index
        membership is updated in the same transaction so a blob is either fully
        published or not visible at all.

        Args:
            data: The exact bytes to store.

        Returns:
            PayloadRef: The content-addressed reference.
        """
        ref = ref_for_payload(data)
        pipe = self.connection.pipeline()
        pipe.set(ref.blob_key, data, nx=True)
        pipe.sadd(BLOB_INDEX_KEY, ref.token)
        pipe.execute()
        return ref

    def contains(self, ref: PayloadRef) -> bool:
        """Whether the blob key currently exists."""
        return bool(self.connection.exists(ref.blob_key))

    def get(self, ref: PayloadRef) -> bytes:
        """Fetch and verify a stored payload.

        Args:
            ref: The payload reference.

        Raises:
            PayloadNotFoundError: If no blob exists for the reference.
            PayloadIntegrityError: If the stored bytes do not hash to the referenced digest.
        """
        data = self.connection.get(ref.blob_key)
        if data is None:
            raise PayloadNotFoundError(f'Offloaded payload does not exist: {ref.token}')
        payload = cast(bytes, data)
        if hashlib.new(ref.algorithm, payload).hexdigest() != ref.digest:
            raise PayloadIntegrityError(f'Checksum mismatch for offloaded payload: {ref.token}')
        return payload

    def resolve(self, marker: bytes) -> bytes:
        """Fetch the payload addressed by a job-hash marker, verifying its checksum."""
        return self.get(PayloadRef.from_marker(marker))

    def iter_refs(self, batch_size: int = 500) -> Iterator[PayloadRef]:
        """Iterate every blob token currently in the index."""
        for token_bytes in self.connection.sscan_iter(BLOB_INDEX_KEY, count=batch_size):
            algorithm, _, digest = as_text(token_bytes).partition(':')
            if algorithm in SUPPORTED_DIGEST_ALGORITHMS and digest:
                yield PayloadRef(algorithm=algorithm, digest=digest)

    def garbage_collect(self, batch_size: int = 500) -> PayloadGCReport:
        """Delete blobs that no existing job hash references (mark-and-sweep).

        Live references are collected from *all* job hashes, so payloads of queued,
        started, deferred, scheduled, finished, failed or canceled jobs are never
        deleted. Job hashes themselves are never modified.

        Concurrent migration is safe: the migrator swaps a job's ``data`` field under
        ``WATCH`` of the blob key, so deleting a blob here aborts any in-flight swap
        that observed it; the migrator then re-uploads and retries, and no job can end
        up pointing at a missing blob.

        Args:
            batch_size: Hint for SCAN and pipelined command batch sizes.

        Returns:
            PayloadGCReport: What was scanned, found live and removed.
        """
        live_tokens: set[str] = set()
        scanned_jobs = 0

        pipe = self.connection.pipeline(transaction=False)
        pending = 0

        def flush() -> None:
            nonlocal pipe, pending
            if not pending:
                return
            values = pipe.execute()
            for value in values:
                if value and is_payload_marker(value):
                    # A malformed marker names no parseable blob, hence nothing to protect.
                    try:
                        live_tokens.add(PayloadRef.from_marker(value).token)
                    except PayloadIntegrityError:
                        logger.warning('Ignoring malformed payload marker during GC', exc_info=True)
            pipe = self.connection.pipeline(transaction=False)
            pending = 0

        for job_key in scan_job_keys(self.connection, batch_size=batch_size):
            scanned_jobs += 1
            pipe.hget(job_key, 'data')
            pending += 1
            if pending >= batch_size:
                flush()
        flush()

        indexed_tokens = [ref.token for ref in self.iter_refs(batch_size=batch_size)]
        dead_tokens = [token for token in indexed_tokens if token not in live_tokens]

        for start in range(0, len(dead_tokens), batch_size):
            chunk = dead_tokens[start : start + batch_size]
            pipe = self.connection.pipeline()
            for token in chunk:
                pipe.delete(BLOB_KEY_PREFIX + token)
            pipe.srem(BLOB_INDEX_KEY, *chunk)
            pipe.execute()

        return PayloadGCReport(
            scanned_jobs=scanned_jobs,
            live_blobs=len(live_tokens),
            indexed_blobs=len(indexed_tokens),
            removed_blobs=len(dead_tokens),
        )
