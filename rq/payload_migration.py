"""Online migration of existing large job payloads to the content store.

The migrator scans job hashes (``rq:job:<id>``) and, for each job whose stored
``data`` field is at least ``min_size`` bytes:

1. verifies the inline payload is readable exactly the way :meth:`Job.restore`
   reads it (zlib envelope + the job serializer);
2. uploads the *verbatim* field bytes to the content store (content-addressed,
   so identical payloads are stored once and shared);
3. swaps the job's ``data`` field to the reference marker atomically, under
   ``WATCH`` of both the job key and the blob key.

Progress (SCAN cursor and counters) is checkpointed in Redis, so an interrupted
run resumes where it left off; per-job work is idempotent and SCAN is
at-least-once. Jobs whose payload cannot be read, whose referenced blob is
missing/corrupt, or whose swap keeps losing a race are reported as failures and
left byte-for-byte untouched — no job is ever left pointing at missing content.
"""

from __future__ import annotations

import hashlib
import logging
import zlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from redis import WatchError
from redis.exceptions import ResponseError

from .defaults import DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD
from .exceptions import (
    PayloadError,
    PayloadIntegrityError,
    PayloadUnreadableError,
)
from .payload_store import (
    PayloadRef,
    PayloadStore,
    is_job_key,
    is_payload_marker,
)
from .serializers import resolve_serializer
from .utils import as_text, decode_redis_hash, now, utcformat

if TYPE_CHECKING:
    from redis import Redis

    from rq.serializers import Serializer

logger = logging.getLogger('rq.payload_migration')

#: Hash key holding the resumable migration checkpoint.
DEFAULT_MIGRATION_STATE_KEY = 'rq:payload:migrate'

JOB_KEY_PREFIX = 'rq:job:'

# Sentinel swap outcomes.
_DONE = 'done'
_CHANGED = 'changed'
_CORRUPT = 'corrupt'
_BUSY = 'busy'


@dataclass
class PayloadMigrationFailure:
    """A single job that could not be migrated and was left unchanged."""

    job_id: str
    error_type: str
    message: str


@dataclass
class PayloadMigrationReport:
    """Outcome of a migration run (counters are cumulative across resumes)."""

    #: Job hashes visited (excluding keys that vanished mid-scan).
    scanned: int = 0
    #: Jobs whose inline payload was swapped for a reference.
    migrated: int = 0
    #: Jobs already carrying a reference (their blob was re-verified).
    already_offloaded: int = 0
    #: Jobs left inline because their payload was smaller than the threshold.
    skipped_small: int = 0
    #: Uploads that reused a blob already stored for another job.
    deduplicated: int = 0
    #: Jobs left untouched because of an explicit payload error.
    failures: list[PayloadMigrationFailure] = field(default_factory=list)
    #: Whether the SCAN completed (cursor back to 0). False when stopped by ``limit``.
    complete: bool = True
    #: Whether this run continued from an earlier checkpoint.
    resumed: bool = False


class PayloadMigrator:
    """Migrates large inline job payloads to a :class:`PayloadStore`."""

    def __init__(
        self,
        connection: Redis,
        *,
        min_size: int = DEFAULT_PAYLOAD_OFFLOAD_THRESHOLD,
        serializer: Serializer | str | None = None,
        batch_size: int = 500,
        state_key: str = DEFAULT_MIGRATION_STATE_KEY,
        max_attempts: int = 5,
    ):
        """
        Args:
            connection: Redis connection.
            min_size: Move a payload only if the stored ``data`` field is at least
                this many bytes long.
            serializer: Serializer used to smoke-test that inline payloads are
                readable before they are moved (defaults to the pickle serializer;
                pass the JSON serializer for queues enqueued with it).
            batch_size: SCAN count hint and checkpoint granularity.
            state_key: Redis hash key holding the resumable checkpoint.
            max_attempts: Maximum retries per job when its swap loses a WATCH race.
        """
        self.connection = connection
        self.min_size = min_size
        self.serializer = resolve_serializer(serializer)
        self.batch_size = batch_size
        self.state_key = state_key
        # SET of job ids already given a final outcome in this sweep. It makes
        # resume exact even though SCAN COUNT is only a hint (one page may already
        # cover the whole keyspace) or a cursor is invalidated by a Redis resize.
        self.done_key = state_key + ':done'
        self.max_attempts = max_attempts
        self.store = PayloadStore(connection)
        self.report = PayloadMigrationReport()

    def migrate(self, *, restart: bool = False, limit: int | None = None) -> PayloadMigrationReport:
        """Run (or resume) the migration.

        Args:
            restart: Delete the checkpoint (including the processed-id set) and
                scan from the beginning.
            limit: If given, stop after processing this many not-yet-processed jobs
                this invocation, leaving the checkpoint in place so a later call
                continues.

        Returns:
            PayloadMigrationReport: Counters (exact, deduplicated across resumes)
            and this invocation's failures.
        """
        if restart:
            self.connection.delete(self.state_key, self.done_key)

        state = decode_redis_hash(self.connection.hgetall(self.state_key), decode_values=True)
        in_progress = bool(state) and not state.get('finished_at')
        if in_progress:
            # Resume an interrupted sweep: SCAN is at-least-once, the done set
            # makes redelivered jobs no-ops and keeps counters exact.
            self.report.resumed = True
            cursor = int(state.get('cursor') or 0)
            self.report.scanned = int(state.get('scanned') or 0)
            self.report.migrated = int(state.get('migrated') or 0)
            self.report.already_offloaded = int(state.get('already_offloaded') or 0)
            self.report.skipped_small = int(state.get('skipped_small') or 0)
            self.report.deduplicated = int(state.get('deduplicated') or 0)
        else:
            # Fresh run (first run, or a rerun after a completed sweep).
            self.connection.delete(self.done_key)
            cursor = 0
            self._write_checkpoint(0, initial=True)

        processed_this_call = 0
        while True:
            resume_cursor = cursor
            try:
                cursor, keys = self.connection.scan(cursor=cursor, match='rq:job:*', count=self.batch_size)
            except ResponseError as exc:
                # SCAN cursors are invalidated by some Redis restarts/resizes.
                if 'invalid cursor' not in str(exc):
                    raise
                logger.warning('Migration cursor invalidated; restarting scan from 0')
                cursor, keys = 0, []
                resume_cursor = 0
                self._write_checkpoint(0)

            cursor = int(cursor)
            for job_key in keys or ():
                if not is_job_key(job_key):
                    continue
                if self._process_job(cast(bytes, job_key)):
                    processed_this_call += 1
                    if limit is not None and processed_this_call >= limit:
                        # Keep the pre-scan cursor so keys skipped within this page
                        # are redelivered; the done set prevents double processing.
                        self._write_checkpoint(resume_cursor)
                        self.report.complete = False
                        return self.report

            self._write_checkpoint(cursor)
            if cursor == 0:
                break

        self.connection.hset(self.state_key, 'finished_at', utcformat(now()))
        self.report.complete = True
        return self.report

    def _write_checkpoint(self, cursor: int, *, initial: bool = False) -> None:
        fields: dict[str, str] = {
            'cursor': str(cursor),
            'scanned': str(self.report.scanned),
            'migrated': str(self.report.migrated),
            'already_offloaded': str(self.report.already_offloaded),
            'skipped_small': str(self.report.skipped_small),
            'deduplicated': str(self.report.deduplicated),
            'updated_at': utcformat(now()),
        }
        if initial:
            fields['started_at'] = utcformat(now())
        # Scalar hset pairs: redis-py 8's mapping= type is incompatible with plain
        # dicts; a pipeline keeps this to a single round trip.
        pipe = self.connection.pipeline(transaction=False)
        for name, value in fields.items():
            pipe.hset(self.state_key, name, value)
        if initial:
            pipe.hdel(self.state_key, 'finished_at')
        pipe.execute()

    def _process_job(self, job_key: bytes) -> bool:
        """Process one job key. Returns False if it was already processed earlier
        in this sweep (redelivered by SCAN), True if work was performed now.
        """
        job_id = as_text(job_key)[len(JOB_KEY_PREFIX) :]
        if self.connection.sismember(self.done_key, job_id):
            return False
        try:
            raw = cast('bytes | None', self.connection.hget(job_key, 'data'))
            status = self._migrate_raw(job_key, raw, attempts=self.max_attempts)
        except PayloadError as exc:
            self._record_failure(job_id, exc)
            return True
        except Exception:
            # Never let one odd job abort the sweep; it is simply left untouched.
            logger.exception('Payload migration failed for job %s', job_id)
            self._record_failure(job_id, PayloadError('Unexpected error while migrating payload'))
            return True

        if status == 'gone':
            # Key expired between SCAN and HGET; nothing to remember.
            return True

        # SADD is the dedup gate for counters: a redelivered job id on resume is
        # never counted twice. Failures are deliberately not added so a later run
        # retries them.
        if not self.connection.sadd(self.done_key, job_id):
            return False
        self.report.scanned += 1
        if status == 'migrated':
            self.report.migrated += 1
        elif status == 'already_offloaded':
            self.report.already_offloaded += 1
        elif status == 'skipped_small':
            self.report.skipped_small += 1
        return True

    def _migrate_raw(self, job_key: bytes, raw: bytes | None, *, attempts: int) -> str:
        if raw is None:
            return 'gone'

        if is_payload_marker(raw):
            # Already migrated: explicitly verify the referenced blob so a missing
            # or corrupted payload is reported instead of silently accepted.
            ref = PayloadRef.from_marker(raw)
            self.store.get(ref)
            return 'already_offloaded'

        if len(raw) < self.min_size:
            return 'skipped_small'

        self._ensure_readable(raw)

        if self.store.contains(PayloadRef('sha256', hashlib.sha256(raw).hexdigest())):
            self.report.deduplicated += 1
        ref = self.store.put(raw)

        outcome, current = self._swap(job_key, raw, ref, attempts=attempts)
        if outcome == _DONE:
            return 'migrated'
        if outcome == _CHANGED:
            # The job was rewritten (e.g. requeued) between read and swap: process
            # its current bytes. Bounded so a hot key can't loop forever.
            if attempts <= 1:
                raise PayloadError('Job changed repeatedly during migration; left unchanged')
            return self._migrate_raw(job_key, current, attempts=attempts - 1)
        if outcome == _CORRUPT:
            raise PayloadIntegrityError(f'Shared blob fails checksum: {ref.token}; job left inline')
        raise PayloadError(f'Payload swap aborted after {self.max_attempts} contended attempts; left unchanged')

    def _ensure_readable(self, raw: bytes) -> None:
        """Mirror Job.restore's envelope handling and validate with the serializer."""
        try:
            decoded = zlib.decompress(raw)
        except zlib.error:
            decoded = raw
        try:
            payload = self.serializer.loads(decoded)
        except Exception as exc:
            raise PayloadUnreadableError('Job payload cannot be loaded by its serializer') from exc
        if not isinstance(payload, tuple | list) or len(payload) != 4:
            raise PayloadUnreadableError('Job payload is not a (func, instance, args, kwargs) tuple')

    def _swap(self, job_key: bytes, raw: bytes, ref: PayloadRef, *, attempts: int) -> tuple[str, bytes | None]:
        """Atomically replace the inline bytes with the marker.

        WATCHing the blob key means a garbage collector that deletes the blob
        aborts this transaction; the blob is then re-uploaded and the swap retried,
        so a marker is never committed while its blob is absent.
        """
        for _ in range(attempts):
            pipe = self.connection.pipeline()
            try:
                pipe.watch(job_key, ref.blob_key)
                current = cast('bytes | None', pipe.hget(job_key, 'data'))
                if current != raw:
                    pipe.unwatch()
                    return _CHANGED, current
                blob = cast('bytes | None', pipe.get(ref.blob_key))
                if blob is None:
                    # The GC (or an eviction) removed it before the swap. Re-publish
                    # and retry; WATCH on the blob key guards the new incarnation too.
                    pipe.unwatch()
                    self.store.put(raw)
                    continue
                if hashlib.new(ref.algorithm, blob).hexdigest() != ref.digest:
                    pipe.unwatch()
                    return _CORRUPT, None
                pipe.multi()
                pipe.hset(job_key, 'data', ref.to_marker())
                pipe.execute()
                return _DONE, None
            except WatchError:
                continue
            finally:
                pipe.reset()
        return _BUSY, None

    def _record_failure(self, job_id: str, exc: Exception) -> None:
        logger.error('Payload migration left job %s untouched: %s', job_id, exc)
        failure = PayloadMigrationFailure(job_id=job_id, error_type=type(exc).__name__, message=str(exc))
        self.report.failures.append(failure)


def migrate_large_payloads(connection: Redis, **kwargs: Any) -> PayloadMigrationReport:
    """Convenience wrapper around :class:`PayloadMigrator`.

    Accepts the same keyword arguments as :meth:`PayloadMigrator.migrate` and the
    :class:`PayloadMigrator` constructor (e.g. ``min_size``, ``serializer``,
    ``restart``, ``limit``).
    """
    migrate_kwargs = {key: kwargs.pop(key) for key in ('restart', 'limit') if key in kwargs}
    return PayloadMigrator(connection, **kwargs).migrate(**migrate_kwargs)


# Re-exported for callers that only import the migration module.
__all__ = [
    'DEFAULT_MIGRATION_STATE_KEY',
    'PayloadMigrationFailure',
    'PayloadMigrationReport',
    'PayloadMigrator',
    'migrate_large_payloads',
]
