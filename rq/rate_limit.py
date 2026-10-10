from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from functools import cached_property

from redis import Redis
from redis.client import Pipeline

from .job import Job, JobStatus
from .utils import as_text, current_timestamp, now, utcformat


class RateLimit:
    """Defines a concurrency-based rate limit for jobs.

    Args:
        key: A string key that groups jobs together for rate limiting.
        concurrency: Maximum number of jobs with this key that can be
            queued or executing at the same time.
    """

    def __init__(self, key: str, concurrency: int):
        if not key:
            raise ValueError('key must not be empty')
        if not isinstance(concurrency, int) or concurrency < 1:
            raise ValueError('concurrency must be an integer of at least 1')
        self.key = key
        self.concurrency = concurrency


# Lua: fill free slots with waiting jobs, oldest first. Each is added to allowed, pushed onto
# its queue (front if enqueue_at_front is set) and marked queued. Returns the promoted job ids.
# Job and queue keys depend on the popped job, so they're built from the passed prefixes.
# KEYS: allowed_key, rate_limited_key
# ARGV: concurrency, timestamp, enqueued_at, queue_key_prefix, job_key_prefix
ACQUIRE_AND_ENQUEUE_SCRIPT = """
local concurrency = tonumber(ARGV[1])
local timestamp = tonumber(ARGV[2])
local enqueued_at = ARGV[3]
local queue_key_prefix = ARGV[4]
local job_key_prefix = ARGV[5]
local promoted = {}

while redis.call('ZCARD', KEYS[1]) < concurrency do
    local result = redis.call('ZPOPMIN', KEYS[2])
    if #result == 0 then
        break
    end
    local job_id = result[1]
    local job_key = job_key_prefix .. job_id
    local fields = redis.call('HMGET', job_key, 'origin', 'status', 'enqueue_at_front')
    local origin, status, enqueue_at_front = fields[1], fields[2], fields[3]
    -- Drop stale entries (missing hash, no origin or not rate_limited); ZPOPMIN removed them.
    if origin and status == 'rate_limited' then
        redis.call('ZADD', KEYS[1], timestamp, job_id)
        if enqueue_at_front == '1' then
            redis.call('LPUSH', queue_key_prefix .. origin, job_id)
        else
            redis.call('RPUSH', queue_key_prefix .. origin, job_id)
        end
        redis.call('HSET', job_key, 'status', 'queued', 'enqueued_at', enqueued_at)
        table.insert(promoted, job_id)
    end
end
return promoted
"""

# Release = remove the completed job from allowed (ARGV[6]) then run the acquire script.
# ARGV: concurrency, timestamp, enqueued_at, queue_key_prefix, job_key_prefix, completed_job_id
RELEASE_AND_ENQUEUE_SCRIPT = "redis.call('ZREM', KEYS[1], ARGV[6])\n" + ACQUIRE_AND_ENQUEUE_SCRIPT

# Like RELEASE_AND_ENQUEUE_SCRIPT, but skips the release and returns an empty list if the job
# is queued or started, so a job that re-acquired its slot after cleanup read its status keeps it.
# KEYS: allowed_key, rate_limited_key, job_key
# ARGV: concurrency, timestamp, enqueued_at, queue_key_prefix, job_key_prefix, job_id
RELEASE_STALE_AND_ENQUEUE_SCRIPT = (
    """
local status = redis.call('HGET', KEYS[3], 'status')
if status == 'queued' or status == 'started' then
    return {}
end
redis.call('ZREM', KEYS[1], ARGV[6])
"""
    + ACQUIRE_AND_ENQUEUE_SCRIPT
)


# Lua: if both allowed and rate_limited sets are empty, drop the key from rq:rl-keys
# and delete the config hash and sorted sets. Returns 1 if cleaned up, 0 if not empty.
# KEYS: allowed_key, rate_limited_key, config_key
# ARGV: rl_keys_key, key
CLEANUP_REGISTRY_SCRIPT = """
if redis.call('ZCARD', KEYS[1]) == 0 and redis.call('ZCARD', KEYS[2]) == 0 then
    redis.call('SREM', ARGV[1], ARGV[2])
    redis.call('DEL', KEYS[1], KEYS[2], KEYS[3])
    return 1
end
return 0
"""


class RateLimitRegistry:
    """Manages the allowed and rate_limited sorted sets for a rate limit key.

    Each rate limit key has:
    - rq:rl:{key} — a hash storing config (e.g., concurrency)
    - rq:rl:{key}:allowed — sorted set of job IDs the limiter has let through
    - rq:rl:{key}:rate_limited — sorted set of job IDs the limiter is holding back
    """

    rl_keys_key = 'rq:rl-keys'

    def __init__(self, key: str, connection: Redis):
        self.key = key
        self.connection = connection
        self._acquire_script = connection.register_script(ACQUIRE_AND_ENQUEUE_SCRIPT)
        self._release_script = connection.register_script(RELEASE_AND_ENQUEUE_SCRIPT)
        self._release_stale_script = connection.register_script(RELEASE_STALE_AND_ENQUEUE_SCRIPT)
        self._cleanup_script = connection.register_script(CLEANUP_REGISTRY_SCRIPT)

    def register(self, concurrency: int, pipeline: Pipeline) -> None:
        """Register this rate limit key and persist its config."""
        pipeline.sadd(self.rl_keys_key, self.key)
        pipeline.hset(self.config_key, 'concurrency', concurrency)

    @cached_property
    def concurrency(self) -> int:
        """Read and cache the concurrency limit from Redis; return 0 if unset."""
        value = self.connection.hget(self.config_key, 'concurrency')
        return int(value) if value else 0

    @classmethod
    def all(cls, connection: Redis) -> list[RateLimitRegistry]:
        """Returns all known RateLimitRegistry instances."""
        keys = connection.smembers(cls.rl_keys_key)
        return [cls(key=as_text(key), connection=connection) for key in keys]

    @classmethod
    def from_job(cls, job: Job) -> RateLimitRegistry:
        """Return a registry for the job's rate limit key and connection."""
        assert job.rate_limit_key
        return cls(key=job.rate_limit_key, connection=job.connection)

    @staticmethod
    def _parse_manifest(rate_limits: Mapping[str, int] | Iterable[RateLimit]) -> dict[str, int]:
        """Validate a manifest and return it as an ordered {key: concurrency} dict.

        Args:
            rate_limits: A mapping of rate limit key to concurrency, or an iterable
                of `RateLimit` (duplicate keys are rejected).

        Returns:
            The manifest as a dict, preserving the input order.

        Raises:
            ValueError: A key is not a non-empty string, a concurrency is not an
                integer of at least 1, or a key appears more than once.
        """
        if isinstance(rate_limits, Mapping):
            entries = list(rate_limits.items())
        else:
            entries = [(rate_limit.key, rate_limit.concurrency) for rate_limit in rate_limits]

        manifest: dict[str, int] = {}
        for key, concurrency in entries:
            if not isinstance(key, str) or not key:
                raise ValueError('rate limit key must be a non-empty string')
            if isinstance(concurrency, bool) or not isinstance(concurrency, int) or concurrency < 1:
                raise ValueError(f'concurrency for {key!r} must be an integer of at least 1')
            if key in manifest:
                raise ValueError(f'duplicate rate limit key: {key!r}')
            manifest[key] = concurrency
        return manifest

    @classmethod
    def reconcile(cls, rate_limits: Mapping[str, int] | Iterable[RateLimit], connection: Redis) -> dict[str, list[str]]:
        """Reconcile registries with an externally maintained manifest of concurrency limits.

        Call at publish time so stored limits follow the manifest:

        - Each listed key is registered, its `concurrency` set from the manifest and
          marked as managed. Waiting jobs are promoted up to the new quota, so raising
          a limit releases the jobs that have been waiting; lowering it admits nothing
          while the key is at or over the new cap (existing jobs keep their slots, and
          later releases read the lowered value).
        - A managed key missing from the manifest is deleted, but only once its allowed
          and rate_limited sets are empty; a non-drained key is kept and retried on the
          next run.
        - Keys created by ad-hoc jobs (never listed in a manifest) are never touched.

        Args:
            rate_limits: A mapping of rate limit key to concurrency, or an iterable of
                `RateLimit`.
            connection: The Redis connection.

        Returns:
            Mapping of each manifest key to the job ids promoted while reconciling it.

        Raises:
            ValueError: The manifest is invalid; no changes are made to any registry.
        """
        # Validate the whole manifest before a single write.
        manifest = cls._parse_manifest(rate_limits)

        # Snapshot existing registries and their managed flags before writing.
        existing = cls.all(connection)
        with connection.pipeline() as pipeline:
            for registry in existing:
                pipeline.hget(registry.config_key, 'managed')
            managed_flags = pipeline.execute()
        managed_keys = {registry.key for registry, flag in zip(existing, managed_flags) if flag is not None}

        # Persist every listed key's config in a single batch.
        registries = {key: cls(key=key, connection=connection) for key in manifest}
        with connection.pipeline() as pipeline:
            for key, concurrency in manifest.items():
                pipeline.sadd(cls.rl_keys_key, key)
                pipeline.hset(registries[key].config_key, mapping={'concurrency': concurrency, 'managed': 1})
            pipeline.execute()

        # Promote waiting jobs into the new quota; each call is one atomic Lua script.
        promoted: dict[str, list[str]] = {}
        for key, concurrency in manifest.items():
            promoted[key] = registries[key].acquire_and_enqueue(concurrency)

        # Keys the manifest previously owned but no longer lists: drop once drained.
        for registry in existing:
            if registry.key in managed_keys and registry.key not in manifest:
                registry._cleanup_script(
                    keys=[registry.allowed_key, registry.rate_limited_key, registry.config_key],
                    args=[cls.rl_keys_key, registry.key],
                )

        return promoted

    @property
    def config_key(self) -> str:
        return f'rq:rl:{self.key}'

    @property
    def allowed_key(self) -> str:
        return f'rq:rl:{self.key}:allowed'

    @property
    def rate_limited_key(self) -> str:
        return f'rq:rl:{self.key}:rate_limited'

    def get_allowed_job_ids(self) -> list[str]:
        """Returns job IDs in the allowed set, ordered by timestamp."""
        return [as_text(job_id) for job_id in self.connection.zrange(self.allowed_key, 0, -1)]

    def get_rate_limited_job_ids(self) -> list[str]:
        """Returns job IDs in the rate_limited set, ordered by timestamp."""
        return [as_text(job_id) for job_id in self.connection.zrange(self.rate_limited_key, 0, -1)]

    def get_allowed_job_count(self) -> int:
        """Returns the number of jobs in the allowed set."""
        return self.connection.zcard(self.allowed_key)

    def get_rate_limited_job_count(self) -> int:
        """Returns the number of jobs in the rate_limited set."""
        return self.connection.zcard(self.rate_limited_key)

    def add_to_rate_limited(self, job_id: str, pipeline: Pipeline, timestamp: float | None = None) -> None:
        """Add a job to the rate_limited set."""
        if timestamp is None:
            timestamp = current_timestamp()
        pipeline.zadd(self.rate_limited_key, {job_id: timestamp})

    def acquire_and_enqueue(
        self, concurrency: int, enqueued_at: datetime | None = None, pipeline: Pipeline | None = None
    ) -> list[str]:
        """Atomically fill free slots with waiting jobs, oldest first.

        Each promoted job is added to allowed, pushed onto its origin queue and marked queued.

        Args:
            concurrency: Maximum number of jobs allowed to be queued or executing simultaneously.
            enqueued_at: Recorded as each promoted job's `enqueued_at`; defaults to now. Pass it
                to mirror the stored value onto an in-memory job without a re-read.
            pipeline: If given, the script is queued on it and runs when the pipeline executes,
                so this returns [] and the promoted ids are in the pipeline's results. If the
                pipeline is watching keys, call `multi()` first.

        Returns:
            The promoted job ids, oldest first.
        """
        from .queue import Queue

        if enqueued_at is None:
            enqueued_at = now()
        timestamp = current_timestamp()
        result = self._acquire_script(
            keys=[self.allowed_key, self.rate_limited_key],
            args=[
                concurrency,
                timestamp,
                utcformat(enqueued_at),
                Queue.redis_queue_namespace_prefix,
                Job.redis_job_namespace_prefix,
            ],
            client=pipeline,
        )
        if pipeline is not None:
            return []
        return [as_text(promoted_id) for promoted_id in result]

    def release_and_enqueue(self, job_id: str) -> list[str]:
        """Atomically release a job's slot, then fill free slots like `acquire_and_enqueue`.

        Args:
            job_id: The job whose slot to release.

        Returns:
            The promoted job ids, oldest first.
        """
        from .queue import Queue

        timestamp = current_timestamp()
        result = self._release_script(
            keys=[self.allowed_key, self.rate_limited_key],
            args=[
                self.concurrency,
                timestamp,
                utcformat(now()),
                Queue.redis_queue_namespace_prefix,
                Job.redis_job_namespace_prefix,
                job_id,
            ],
        )
        return [as_text(promoted_id) for promoted_id in result]

    def release_stale_and_enqueue(self, job_id: str) -> list[str]:
        """Atomically release a job's slot unless it is queued or started, then fill free
        slots like `acquire_and_enqueue`.

        Returns:
            The promoted job ids, oldest first.
        """
        from .queue import Queue

        timestamp = current_timestamp()
        result = self._release_stale_script(
            keys=[self.allowed_key, self.rate_limited_key, Job.key_for(job_id)],
            args=[
                self.concurrency,
                timestamp,
                utcformat(now()),
                Queue.redis_queue_namespace_prefix,
                Job.redis_job_namespace_prefix,
                job_id,
            ],
        )
        return [as_text(promoted_id) for promoted_id in result]

    def cancel(self, job_id: str, pipeline: Pipeline | None = None) -> list[str]:
        """Remove a job from rate limit tracking and, if it held a slot, fill free slots
        with waiting jobs.

        Args:
            job_id: The job ID to remove.
            pipeline: If given, only the removals are queued on it; promotion waits for a later
                release or cleanup.

        Returns:
            The promoted job ids; always [] with a pipeline.
        """
        if pipeline is not None:
            pipeline.zrem(self.allowed_key, job_id)
            pipeline.zrem(self.rate_limited_key, job_id)
            return []

        was_allowed = self.connection.zrem(self.allowed_key, job_id)
        self.connection.zrem(self.rate_limited_key, job_id)
        if was_allowed:
            return self.acquire_and_enqueue(self.concurrency)
        return []

    def _release_stale_allowed_jobs(self) -> None:
        """Free allowed slots whose job no longer exists or is not in a state
        that legitimately holds a slot (queued or started).

        Any other state — missing, terminal, scheduled or malformed — means the
        slot leaked and should be freed so rate_limited jobs can proceed.
        """
        allowed_statuses = (JobStatus.QUEUED, JobStatus.STARTED)
        job_ids = self.get_allowed_job_ids()
        if not job_ids:
            return

        # Read only the status field — hydrating a full Job (Job.restore) raises on a
        # malformed status; here an unknown/missing status is just treated as stale.
        with self.connection.pipeline() as pipeline:
            for job_id in job_ids:
                pipeline.hget(Job.key_for(job_id), 'status')
            raw_statuses = pipeline.execute()

        for job_id, raw_status in zip(job_ids, raw_statuses):
            status = as_text(raw_status) if raw_status else None
            if status not in allowed_statuses:
                # The status read above may be stale by now; the script re-checks it.
                self.release_stale_and_enqueue(job_id)

    def cleanup(self) -> None:
        """Free stale allowed slots, fill free slots with waiting jobs, then remove the
        registry if both allowed and rate_limited are empty.

        Called during periodic maintenance to handle cases where jobs are stuck
        in rate_limited (e.g., worker crashed before releasing capacity) or where a
        job left the allowed set holding a slot it should have released.
        """
        self._release_stale_allowed_jobs()

        if self.concurrency and self.acquire_and_enqueue(self.concurrency):
            return

        # Atomically remove registry if empty
        self._cleanup_script(
            keys=[self.allowed_key, self.rate_limited_key, self.config_key],
            args=[self.rl_keys_key, self.key],
        )


def release_slot(job: Job) -> list[str]:
    """Release the job's slot unless its cached status is queued or started, and
    promote waiting jobs.

    Call after the transaction setting the outcome commits, with the job's
    cached status matching that outcome.

    Returns the promoted job ids.
    """
    if not job.has_rate_limit or job.get_status(refresh=False) in (JobStatus.QUEUED, JobStatus.STARTED):
        return []
    return RateLimitRegistry.from_job(job).release_and_enqueue(job.id)
