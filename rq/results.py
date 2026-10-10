from __future__ import annotations

import zlib
from base64 import b64decode, b64encode
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from redis import Redis
from redis.exceptions import ResponseError

from .defaults import UNSERIALIZABLE_RETURN_VALUE_PAYLOAD
from .job import Job
from .serializers import resolve_serializer
from .utils import decode_redis_hash, now


def get_key(job_id):
    return f'rq:results:{job_id}'


def stream_id_to_tuple(stream_id: str) -> tuple[int, int]:
    """Convert a Redis stream ID ("<ms>-<seq>") into a comparable tuple."""
    milliseconds, sequence = stream_id.split('-', 1)
    return int(milliseconds), int(sequence)


class ResultStreamGapError(Exception):
    """Raised when entries after a consumer's checkpoint are no longer in the stream.

    The result stream was trimmed (MAXLEN retention) or expired between the
    consumer's last acknowledged position and the oldest entry that is still
    available. The gap is reported instead of being silently skipped.

    Recover explicitly with ResultConsumer.reset() (replay from the oldest
    surviving entry) or ResultConsumer.seek() (jump to a surviving entry).
    """

    def __init__(self, consumer: str, last_id: str, first_available_id: str):
        self.consumer = consumer
        self.last_id = last_id
        self.first_available_id = first_available_id
        super().__init__(
            f'Result stream gap detected for consumer {consumer!r}: entries after '
            f'{last_id} were trimmed or expired, oldest available entry is '
            f'{first_available_id}. Use ResultConsumer.reset() or seek() to recover.'
        )


class Result:
    class Type(Enum):
        SUCCESSFUL = 1
        FAILED = 2
        STOPPED = 3
        RETRIED = 4
        MAX_RETRIES_EXCEEDED = 5

    def __init__(
        self,
        job_id: str,
        type: Type,
        connection: Redis,
        id: str | None = None,
        created_at: datetime | None = None,
        return_value: Any | None = None,
        exc_string: str | None = None,
        worker_name: str = '',
        serializer=None,
        execution_id: str | None = None,
        execution_started_at: datetime | None = None,
        execution_ended_at: datetime | None = None,
    ):
        self.return_value = return_value
        self.exc_string = exc_string
        self.type = type
        self.created_at = created_at if created_at else now()
        self.serializer = resolve_serializer(serializer)
        self.connection = connection
        self.job_id = job_id
        self.id = id
        self.worker_name = worker_name
        self.execution_id = execution_id
        self.execution_started_at = execution_started_at
        self.execution_ended_at = execution_ended_at

    def __repr__(self):
        return f'Result(id={self.id}, type={self.Type(self.type).name})'

    def __eq__(self, other):
        try:
            return self.id == other.id
        except AttributeError:
            return False

    def __bool__(self):
        return bool(self.id)

    @classmethod
    def create(
        cls,
        job,
        type,
        ttl,
        return_value=None,
        exc_string=None,
        worker_name='',
        pipeline=None,
        execution_id: str | None = None,
        execution_started_at: datetime | None = None,
        execution_ended_at: datetime | None = None,
    ) -> Result:
        result = cls(
            job_id=job.id,
            type=type,
            connection=job.connection,
            return_value=return_value,
            exc_string=exc_string,
            worker_name=worker_name,
            serializer=job.serializer,
            execution_id=execution_id,
            execution_started_at=execution_started_at,
            execution_ended_at=execution_ended_at,
        )
        result.save(ttl=ttl, pipeline=pipeline)
        return result

    @classmethod
    def create_failure(
        cls,
        job,
        ttl,
        exc_string,
        worker_name='',
        pipeline=None,
        execution_id: str | None = None,
        execution_started_at: datetime | None = None,
        execution_ended_at: datetime | None = None,
    ) -> Result:
        result = cls(
            job_id=job.id,
            type=cls.Type.FAILED,
            connection=job.connection,
            exc_string=exc_string,
            worker_name=worker_name,
            serializer=job.serializer,
            execution_id=execution_id,
            execution_started_at=execution_started_at,
            execution_ended_at=execution_ended_at,
        )
        result.save(ttl=ttl, pipeline=pipeline)
        return result

    @classmethod
    def create_retried(
        cls,
        job,
        ttl,
        return_value,
        worker_name,
        execution_id: str,
        execution_started_at: datetime,
        execution_ended_at: datetime,
        pipeline=None,
    ) -> Result:
        return cls.create(
            job,
            cls.Type.RETRIED,
            ttl,
            return_value=return_value,
            worker_name=worker_name,
            pipeline=pipeline,
            execution_id=execution_id,
            execution_started_at=execution_started_at,
            execution_ended_at=execution_ended_at,
        )

    @classmethod
    def create_max_retries_exceeded(
        cls,
        job,
        ttl,
        return_value,
        worker_name,
        execution_id: str,
        execution_started_at: datetime,
        execution_ended_at: datetime,
        pipeline=None,
    ) -> Result:
        return cls.create(
            job,
            cls.Type.MAX_RETRIES_EXCEEDED,
            ttl,
            return_value=return_value,
            worker_name=worker_name,
            pipeline=pipeline,
            execution_id=execution_id,
            execution_started_at=execution_started_at,
            execution_ended_at=execution_ended_at,
        )

    @classmethod
    def all(cls, job: Job, serializer=None):
        """Returns all results for job"""
        # response = job.connection.zrange(cls.get_key(job.id), 0, 10, desc=True, withscores=True)
        response = job.connection.xrevrange(cls.get_key(job.id), '+', '-')
        results = []
        for result_id, payload in response:
            results.append(
                cls.restore(job.id, result_id.decode(), payload, connection=job.connection, serializer=serializer)
            )

        return results

    @classmethod
    def count(cls, job: Job) -> int:
        """Returns the number of job results"""
        return job.connection.xlen(cls.get_key(job.id))

    @classmethod
    def delete_all(cls, job: Job) -> None:
        """Delete all job results and consumer checkpoints"""
        job.connection.delete(cls.get_key(job.id), ResultConsumer.get_key(job.id))

    @classmethod
    def restore(cls, job_id: str, result_id: str, payload: dict, connection: Redis, serializer=None) -> Result:
        """Create a Result object from given Redis payload"""
        created_at = datetime.fromtimestamp(int(result_id.split('-')[0]) / 1000, tz=timezone.utc)
        payload = decode_redis_hash(payload)
        # data, timestamp = payload
        # result_data = json.loads(data)
        # created_at = datetime.fromtimestamp(timestamp, tz=timezone.utc)

        serializer = resolve_serializer(serializer)
        return_value = payload.get('return_value')
        if return_value is not None:
            return_value = serializer.loads(b64decode(return_value.decode()))

        exc_string = payload.get('exc_string')
        if exc_string:
            exc_string = zlib.decompress(b64decode(exc_string)).decode()

        worker_name = payload.get('worker_name', b'').decode() if payload.get('worker_name') else ''

        execution_id = payload.get('execution_id')
        if execution_id:
            execution_id = execution_id.decode()

        execution_started_at = payload.get('execution_started_at')
        if execution_started_at:
            execution_started_at = datetime.fromtimestamp(float(execution_started_at), tz=timezone.utc)

        execution_ended_at = payload.get('execution_ended_at')
        if execution_ended_at:
            execution_ended_at = datetime.fromtimestamp(float(execution_ended_at), tz=timezone.utc)

        return Result(
            job_id,
            Result.Type(int(payload['type'])),
            connection=connection,
            id=result_id,
            created_at=created_at,
            return_value=return_value,
            exc_string=exc_string,
            worker_name=worker_name,
            execution_id=execution_id,
            execution_started_at=execution_started_at,
            execution_ended_at=execution_ended_at,
        )

    @classmethod
    def fetch(cls, job: Job, serializer=None) -> Result | None:
        """Fetch a result that matches a given job ID. The current sorted set
        based implementation does not allow us to fetch a given key by ID
        so we need to iterate through results, deserialize the payload and
        look for a matching ID.

        Future Redis streams based implementation may make this more efficient
        and scalable.
        """
        return None

    @classmethod
    def fetch_latest(cls, job: Job, serializer=None, timeout: int = 0) -> Result | None:
        """Returns the latest result for given job instance or ID.

        If a non-zero timeout is provided, block for a result until timeout is reached.
        """
        if timeout:
            # Unlike blpop, xread timeout is in miliseconds. "0-0" is the special value for the
            # first item in the stream, like '-' for xrevrange.
            timeout_ms = timeout * 1000
            response = job.connection.xread({cls.get_key(job.id): '0-0'}, block=timeout_ms)
            if not response:
                return None
            response = response[0]  # Querying single stream only.
            response = response[1]  # Xread also returns Result.id, which we don't need.
            result_id, payload = response[-1]  # Take most recent result.

        else:
            # If not blocking, use xrevrange to load a single result (as xread will load them all).
            response = job.connection.xrevrange(cls.get_key(job.id), '+', '-', count=1)
            if not response:
                return None
            result_id, payload = response[0]

        res = cls.restore(job.id, result_id.decode(), payload, connection=job.connection, serializer=serializer)
        return res

    @classmethod
    def get_key(cls, job_id):
        return f'rq:results:{job_id}'

    def save(self, ttl, pipeline=None):
        """Save result data to Redis"""
        key = self.get_key(self.job_id)

        connection = pipeline if pipeline is not None else self.connection
        # result = connection.zadd(key, {self.serialize(): self.created_at.timestamp()})
        result = connection.xadd(key, self.serialize(), maxlen=10)
        # If xadd() is called in a pipeline, it returns a pipeline object instead of stream ID
        if pipeline is None:
            self.id = result.decode()
        if ttl is not None:
            if ttl == -1:
                connection.persist(key)
            else:
                connection.expire(key, ttl)
        return self.id

    def serialize(self) -> dict[str, Any]:
        data: dict[str, Any] = {'type': self.type.value, 'worker_name': self.worker_name}

        if self.exc_string is not None:
            data['exc_string'] = b64encode(zlib.compress(self.exc_string.encode())).decode()

        try:
            serialized = self.serializer.dumps(self.return_value)
        except:  # noqa
            serialized = self.serializer.dumps(UNSERIALIZABLE_RETURN_VALUE_PAYLOAD)

        if self.return_value is not None:
            data['return_value'] = b64encode(serialized).decode()

        if self.execution_id:
            data['execution_id'] = self.execution_id

        if self.execution_started_at:
            data['execution_started_at'] = self.execution_started_at.timestamp()

        if self.execution_ended_at:
            data['execution_ended_at'] = self.execution_ended_at.timestamp()

        # return json.dumps(data)
        return data


class ResultConsumer:
    """Incrementally reads a job's result stream with a durable per-consumer position.

    Each named consumer owns an independent checkpoint (the ID of the last
    successfully processed result), stored in a Redis hash. Delivery is
    at-least-once: :meth:`read` returns entries strictly newer than the
    checkpoint without advancing it, and :meth:`ack` persists progress only
    after processing succeeded. If processing fails or the process dies
    before ``ack()``, the same entries are redelivered on the next ``read()``;
    reprocessing the same batch converges to the same checkpoint. Checkpoints
    of different consumer names never affect each other.

    If retention (MAXLEN trimming or stream expiry) removed entries past the
    checkpoint, :meth:`read` raises :class:`ResultStreamGapError` rather than
    silently skipping them. Recover with :meth:`reset` (replay oldest
    surviving entries) or :meth:`seek` (jump to a surviving entry).
    """

    def __init__(self, job: Job, name: str, connection: Redis | None = None, serializer=None):
        if not isinstance(name, str) or not name:
            raise ValueError('ResultConsumer name must be a non-empty string')
        self.job = job
        self.job_id = job.id
        self.name = name
        self.connection = connection if connection is not None else job.connection
        self.serializer = resolve_serializer(serializer if serializer is not None else job.serializer)
        # ID of the last entry returned by read(); used by ack() without args.
        self._last_read_id: str | None = None

    def __repr__(self):
        return f'ResultConsumer(job_id={self.job_id}, name={self.name!r})'

    @classmethod
    def get_key(cls, job_id):
        """Redis hash mapping consumer names to their last acknowledged stream ID."""
        return f'rq:results:{job_id}:positions'

    @property
    def stream_key(self) -> str:
        return Result.get_key(self.job_id)

    @property
    def positions_key(self) -> str:
        return self.get_key(self.job_id)

    def position(self) -> str | None:
        """Last acknowledged stream ID for this consumer, or None if unregistered."""
        raw = self.connection.hget(self.positions_key, self.name)
        return raw.decode() if raw is not None else None

    def read(self, count: int | None = None, block: int = 0) -> list[Result]:
        """Read results newer than this consumer's checkpoint.

        The checkpoint is not advanced; call :meth:`ack` after the returned
        results have been processed successfully.

        Args:
            count: Maximum number of results to return.
            block: Seconds to block waiting for new results. 0 (the default)
                returns immediately when there is nothing new.

        Returns:
            Results in append order, oldest first; empty list when up to date.

        Raises:
            ResultStreamGapError: Entries after the checkpoint were trimmed
                or expired and can no longer be delivered.
        """
        position = self.position()
        # A consumer without a checkpoint starts at the beginning of the stream.
        start_id = position if position is not None else '0-0'
        if start_id != '0-0':
            self._raise_if_gap(start_id)

        read_kwargs: dict[str, Any] = {}
        if count is not None:
            read_kwargs['count'] = count
        if block:
            # XREAD block timeout is in milliseconds.
            read_kwargs['block'] = int(block * 1000)

        try:
            response = self.connection.xread({self.stream_key: start_id}, **read_kwargs)
        except ResponseError:
            # Some Redis versions/forks reject XREAD from an ID below the
            # stream minimum instead of silently returning the new head.
            self._raise_if_gap(start_id)
            raise

        results: list[Result] = []
        if response:
            entries = response[0][1]  # Single stream queried: (key, entries).
            for result_id, payload in entries:
                results.append(
                    Result.restore(
                        self.job_id,
                        result_id.decode(),
                        payload,
                        connection=self.connection,
                        serializer=self.serializer,
                    )
                )
            if results:
                self._last_read_id = results[-1].id
        return results

    def ack(self, result_id: str | None = None) -> str:
        """Persist progress after results were processed successfully.

        Args:
            result_id: Stream ID to acknowledge. Defaults to the last ID
                returned by :meth:`read` on this instance.

        Returns:
            The acknowledged stream ID.

        Raises:
            ValueError: Nothing to acknowledge, or moving the checkpoint
                backwards is attempted.
        """
        if result_id is None:
            result_id = self._last_read_id
        if result_id is None:
            raise ValueError('Nothing to acknowledge; process results from read() first')
        if isinstance(result_id, bytes):
            result_id = result_id.decode()
        self._advance(result_id)
        return result_id

    def seek(self, result_id: str) -> str:
        """Explicitly move the checkpoint to ``result_id`` (gap recovery).

        The next :meth:`read` returns entries strictly newer than ``result_id``.
        Moving backwards is rejected; use :meth:`reset` to replay from the
        oldest surviving entry.
        """
        if isinstance(result_id, bytes):
            result_id = result_id.decode()
        self._advance(result_id)
        return result_id

    def reset(self) -> None:
        """Forget the checkpoint, accepting any gap and replaying from the
        oldest surviving entry on the next :meth:`read`."""
        self.connection.hdel(self.positions_key, self.name)
        self._last_read_id = None

    def _raise_if_gap(self, position: str) -> None:
        """Raise if entries strictly between position and the stream head are gone."""
        # Stream does not exist yet (or fully expired): nothing to compare.
        # If it is recreated later with newer IDs, the comparison below catches it.
        if not self.connection.exists(self.stream_key):
            return
        first_entry = self.connection.xinfo_stream(self.stream_key).get('first-entry')
        if first_entry is None:
            return
        first_id = first_entry[0]
        if isinstance(first_id, bytes):
            first_id = first_id.decode()
        if stream_id_to_tuple(position) < stream_id_to_tuple(first_id):
            raise ResultStreamGapError(self.name, position, first_id)

    def _advance(self, result_id: str) -> None:
        """Persist a strictly monotonic checkpoint, mirroring stream TTL."""
        current = self.position()
        if current is not None and stream_id_to_tuple(current) > stream_id_to_tuple(result_id):
            raise ValueError(
                f'Cannot move consumer {self.name!r} backwards: checkpoint is at '
                f'{current}, refusing to record {result_id}'
            )
        pipeline = self.connection.pipeline()
        pipeline.hset(self.positions_key, self.name, result_id)
        # Keep the checkpoints alive as long as the stream they point into.
        ttl = self.connection.pttl(self.stream_key)
        if ttl > 0:
            pipeline.pexpire(self.positions_key, ttl)
        elif ttl == -1:
            pipeline.persist(self.positions_key)
        pipeline.execute()
