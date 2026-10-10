"""Resumable migration of pending jobs between queues.

``migrate_queues()`` renames/consolidates queues according to an old-name ->
new-name mapping. Only jobs that are still waiting on a queue (the job IDs in
``rq:queue:{name}``) are moved; jobs already dequeued by a worker (in the
intermediate queue) or currently executing (in the started job registry) are
left exactly where they are, as are queues that don't appear in the mapping.

Jobs kept in other per-queue registries -- deferred, ready-to-enqueue,
scheduled, rate-limited, finished, failed and canceled -- are not part of the
queue list, so they stay registered under their original queue name; migrate a
queue only once those registries have drained (or re-register them separately).

Every moved job keeps its ID and its FIFO position and is re-attributed to the
new queue (its ``origin`` field is rewritten), so workers listening on the new
queue consume it in the same order as before. When several source queues merge
into one target, the sources are concatenated in lexicographic source-name
order; each source keeps its original FIFO order.

The migration is validated up front and is all-or-nothing: an unreasonable
mapping or a target queue that already holds pending jobs aborts the whole
operation before anything is written. Progress is recorded in a Redis ledger,
keyed deterministically by the mapping, so re-running an interrupted migration
resumes where it stopped without moving a job twice or losing one.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from collections.abc import Mapping
from typing import TYPE_CHECKING, NamedTuple, cast

from .exceptions import (
    InvalidQueueMappingError,
    QueueMigrationConflictError,
    QueueMigrationInProgressError,
)
from .job import Job
from .queue import Queue
from .scripts import acquire_or_refresh_lock, release_lock
from .utils import as_text, get_version, now

if TYPE_CHECKING:
    from redis import Redis

logger = logging.getLogger('rq.queue_migration')

#: Namespace for the ledger hash, per-source staging lists and the migration lock.
MIGRATION_KEY_PREFIX = 'rq:queue-migration:'
LEDGER_KEY_TEMPLATE = f'{MIGRATION_KEY_PREFIX}{{migration_id}}'
STAGING_KEY_TEMPLATE = f'{MIGRATION_KEY_PREFIX}{{migration_id}}:staging:{{source}}'
MIGRATION_LOCK_KEY = f'{MIGRATION_KEY_PREFIX}lock'

#: The lock auto-expires in case the migrating process is killed; it is renewed
#: on every drain batch while a migration is actively running.
MIGRATION_LOCK_TTL = 60
#: Number of per-job transfers sent in one non-transactional pipeline batch.
DRAIN_BATCH_SIZE = 200
#: Completed ledgers are kept for audit/idempotency and then expire.
LEDGER_TTL_SECONDS = 7 * 24 * 3600

STAGE_STAGED = 'staged'
STAGE_DONE = 'done'
STATE_IN_PROGRESS = 'in_progress'
STATE_COMPLETE = 'complete'


class QueueMigrationResult(NamedTuple):
    """Outcome of a queue migration run.

    Attributes:
        migration_id: Stable identifier derived from the mapping; identical for
            every resume run of the same mapping.
        queue_mapping: The normalized source -> target mapping that was applied.
        moved_jobs: Number of pending jobs moved out of each source queue.
        resumed: True when the run continued (or found already finished) a
            migration recorded by an earlier, possibly interrupted, run.
    """

    migration_id: str
    queue_mapping: dict[str, str]
    moved_jobs: dict[str, int]
    resumed: bool


# Atomically detach a source queue list from its name. The whole list is
# renamed to a staging key in one operation: jobs dequeued afterwards come from
# producers that enqueued after the snapshot and stay on the old queue.
STAGE_SOURCE_SCRIPT = """
    -- KEYS[1] = source queue list (rq:queue:{source})
    -- KEYS[2] = staging list
    -- KEYS[3] = ledger hash
    -- ARGV[1] = source queue name
    if redis.call('EXISTS', KEYS[2]) == 1 then
        return redis.error_reply('queue migration staging key already exists')
    end
    if redis.call('EXISTS', KEYS[1]) == 1 then
        redis.call('RENAME', KEYS[1], KEYS[2])
    end
    redis.call('HSET', KEYS[3], 'stage:' .. ARGV[1], 'staged')
    return 1
"""

# Move a single job from the staging list to the target queue and re-attribute
# it. The transfer is atomic: after each call the job id is either still in
# staging or already in the target queue, so a crash at any point neither
# duplicates nor loses the id. The origin rewrite happens in the same atomic
# call.
#
# ARGV[3] selects the move command:
#   * "LEFT RIGHT" uses LMOVE to append the staging list's head to the target's
#     tail (Redis >= 6.2), a straightforward FIFO append;
#   * "RPOPLPUSH" is the legacy fallback. It pushes to the target's head, so on
#     legacy Redis sources sharing a target must be drained in reverse
#     source-name order to end up in the same documented concatenation order.
TRANSFER_JOB_SCRIPT = """
    -- KEYS[1] = staging list
    -- KEYS[2] = target queue list (rq:queue:{target})
    -- ARGV[1] = job key prefix (rq:job:)
    -- ARGV[2] = target queue name
    -- ARGV[3] = move mode ("LR" for LMOVE LEFT RIGHT, "RP" for RPOPLPUSH)
    local job_id
    if ARGV[3] == 'LR' then
        job_id = redis.call('LMOVE', KEYS[1], KEYS[2], 'LEFT', 'RIGHT')
    else
        job_id = redis.call('RPOPLPUSH', KEYS[1], KEYS[2])
    end
    if not job_id then
        return 0
    end
    local job_key = ARGV[1] .. job_id
    if redis.call('EXISTS', job_key) == 1 then
        redis.call('HSET', job_key, 'origin', ARGV[2])
    end
    return 1
"""

# Finish a source: its staging list must be fully drained, its queue
# registration is swapped for the target's (the source registration is removed
# only when no late jobs recreated the source list), and the ledger records the
# source as done.
FINALIZE_SOURCE_SCRIPT = """
    -- KEYS[1] = staging list
    -- KEYS[2] = source queue list
    -- KEYS[3] = registered queues set (rq:queues)
    -- KEYS[4] = ledger hash
    -- ARGV[1] = source queue name
    -- ARGV[2] = source queue key (rq:queue:{source})
    -- ARGV[3] = target queue key (rq:queue:{target})
    if redis.call('LLEN', KEYS[1]) ~= 0 then
        return redis.error_reply('queue migration staging list is not empty')
    end
    redis.call('DEL', KEYS[1])
    if redis.call('LLEN', KEYS[2]) == 0 then
        redis.call('SREM', KEYS[3], ARGV[2])
    end
    redis.call('SADD', KEYS[3], ARGV[3])
    redis.call('HSET', KEYS[4], 'stage:' .. ARGV[1], 'done')
    return 1
"""


def _migration_id(queue_mapping: Mapping[str, str]) -> str:
    """Stable ID for a mapping, so reruns of the same mapping resume the same ledger."""
    payload = json.dumps(queue_mapping, sort_keys=True, separators=(',', ':')).encode('utf-8')
    return hashlib.sha1(payload).hexdigest()[:12]


def _validate_mapping(queue_mapping: Mapping[str, str] | None) -> dict[str, str]:
    """Validate and normalize a queue migration mapping.

    Raises ``InvalidQueueMappingError`` without touching Redis when the mapping
    is missing/empty, uses non-string or empty names, maps a queue to itself, or
    uses the same queue name as both a source and a target (a chained rename
    like ``a->b, b->c`` or a swap), which cannot be applied unambiguously in
    one pass.
    """
    if not isinstance(queue_mapping, Mapping) or not queue_mapping:
        raise InvalidQueueMappingError(
            'queue_mapping must be a non-empty mapping of source queue names to target queue names'
        )

    normalized: dict[str, str] = {}
    for source, target in queue_mapping.items():
        if not isinstance(source, str) or not source:
            raise InvalidQueueMappingError(f'Invalid source queue name: {source!r}')
        if not isinstance(target, str) or not target:
            raise InvalidQueueMappingError(f'Invalid target queue name for queue {source!r}: {target!r}')
        if source == target:
            raise InvalidQueueMappingError(f'Cannot migrate queue {source!r} onto itself; remove it from the mapping')
        normalized[source] = target

    sources = set(normalized)
    targets = set(normalized.values())
    overlap = sources & targets
    if overlap:
        raise InvalidQueueMappingError(
            f'Queue(s) {sorted(overlap)} are both migration sources and targets; '
            'apply chained renames in separate migrations'
        )
    return normalized


def _conflicting_targets(connection: Redis, targets: set[str]) -> list[tuple[str, int]]:
    """Return (target, count) for target queues that already hold pending jobs."""
    target_keys = [Queue.redis_queue_namespace_prefix + name for name in targets]
    with connection.pipeline(transaction=False) as pipeline:
        for key in target_keys:
            pipeline.llen(key)
        counts = pipeline.execute()
    return [(name, cast(int, count)) for name, count in zip(sorted(targets), counts) if cast(int, count) > 0]


def migrate_queues(
    queue_mapping: Mapping[str, str],
    connection: Redis,
    job_class: type[Job] | None = None,
) -> QueueMigrationResult:
    """Move pending jobs from source queues to target queues per ``queue_mapping``.

    See the module docstring for the full semantics. In short:

    * only not-yet-started (queued) jobs of the mapped source queues move, in
      FIFO order, with their ``origin`` rewritten to the target queue;
    * several sources may merge into one target;
    * an invalid mapping or a non-empty target queue aborts the entire
      migration with an exception before any write;
    * the migration is crash-safe and resumable: rerunning the same mapping
      after an interruption finishes the remaining work without duplicates or
      lost jobs, and a completed migration rerun is a successful no-op;
    * jobs being dequeued or executed, and queues absent from the mapping, are
      left untouched.

    Args:
        queue_mapping: Mapping of existing queue names to new queue names.
        connection: Redis connection.
        job_class: Job class to use; only matters for custom job key prefixes.

    Returns:
        QueueMigrationResult: The migration ID, mapping, per-source moved
            counts and whether an existing ledger was resumed.

    Raises:
        InvalidQueueMappingError: The mapping is missing or unreasonable.
        QueueMigrationConflictError: A target queue already has pending jobs.
        QueueMigrationInProgressError: Another migration is currently running.
    """
    mapping = _validate_mapping(queue_mapping)
    job_cls: type[Job] = job_class or Job

    migration_id = _migration_id(mapping)
    ledger_key = LEDGER_KEY_TEMPLATE.format(migration_id=migration_id)
    lock_token = uuid.uuid4().hex

    lock_state = acquire_or_refresh_lock(connection, MIGRATION_LOCK_KEY, lock_token, MIGRATION_LOCK_TTL)
    if lock_state == 'taken':
        raise QueueMigrationInProgressError(
            'Another queue migration is in progress; wait for it to finish or retry after the lock expires'
        )

    try:
        resumed = bool(connection.exists(ledger_key))
        raw_state = connection.hget(ledger_key, 'state')
        if resumed and raw_state is not None and as_text(raw_state) == STATE_COMPLETE:
            logger.info('Queue migration %s already completed; nothing to do', migration_id)
            stored_counts = _stored_moved_counts(connection, ledger_key, mapping)
            return QueueMigrationResult(migration_id, mapping, stored_counts, True)

        if not resumed:
            # Pre-flight conflict detection. This runs only for a fresh migration;
            # targets legitimately contain migrated jobs when resuming one.
            conflicts = _conflicting_targets(connection, set(mapping.values()))
            if conflicts:
                details = ', '.join(
                    f'{name} ({count} pending job{"s" if count != 1 else ""})' for name, count in conflicts
                )
                raise QueueMigrationConflictError(
                    f'Refusing to migrate: target queue(s) already contain pending jobs: {details}'
                )
            timestamp = now().isoformat()
            connection.hset(
                ledger_key,
                mapping={
                    'mapping': json.dumps(mapping, sort_keys=True),
                    'state': STATE_IN_PROGRESS,
                    'created_at': timestamp,
                    'updated_at': timestamp,
                },
            )

        moved_jobs = _run_migration(connection, mapping, migration_id, ledger_key, job_cls, lock_token)
        connection.hset(ledger_key, mapping={'state': STATE_COMPLETE, 'updated_at': now().isoformat()})
        connection.expire(ledger_key, LEDGER_TTL_SECONDS)
        logger.info('Queue migration %s finished, moved %d job(s)', migration_id, sum(moved_jobs.values()))
        return QueueMigrationResult(migration_id, mapping, moved_jobs, resumed)
    finally:
        release_lock(connection, MIGRATION_LOCK_KEY, lock_token)


def _stored_moved_counts(connection: Redis, ledger_key: str, mapping: Mapping[str, str]) -> dict[str, int]:
    with connection.pipeline(transaction=False) as pipeline:
        for source in mapping:
            pipeline.hget(ledger_key, f'moved:{source}')
        values = pipeline.execute()
    return {source: int(value) if value is not None else 0 for source, value in zip(mapping, values)}


def _run_migration(
    connection: Redis,
    mapping: Mapping[str, str],
    migration_id: str,
    ledger_key: str,
    job_class: type[Job],
    lock_token: str,
) -> dict[str, int]:
    """Drain every source queue, skipping sources a previous run finished."""
    stage_script = connection.register_script(STAGE_SOURCE_SCRIPT)
    transfer_script = connection.register_script(TRANSFER_JOB_SCRIPT)
    finalize_script = connection.register_script(FINALIZE_SOURCE_SCRIPT)

    # LMOVE (6.2+) appends head->tail directly. The RPOPLPUSH fallback pushes to
    # the target's head, so sources sharing a target are drained in reverse
    # source-name order on legacy servers to yield the same final order.
    use_lmove = get_version(connection) >= (6, 2, 0)
    move_mode = 'LR' if use_lmove else 'RP'

    moved_jobs: dict[str, int] = {}
    # Sources sharing a target are processed in source-name order, giving the
    # merged target a deterministic, documented concatenation order.
    ordered_sources = sorted(mapping, key=lambda source: (mapping[source], source), reverse=not use_lmove)

    for source in ordered_sources:
        target = mapping[source]
        stage = connection.hget(ledger_key, f'stage:{source}')
        if stage is not None:
            stage = as_text(stage)

        if stage == STAGE_DONE:
            stored = connection.hget(ledger_key, f'moved:{source}')
            moved_jobs[source] = int(stored) if stored is not None else 0
            continue

        source_key = Queue.redis_queue_namespace_prefix + source
        target_key = Queue.redis_queue_namespace_prefix + target
        staging_key = STAGING_KEY_TEMPLATE.format(migration_id=migration_id, source=source)

        if stage != STAGE_STAGED:
            stage_script(keys=[source_key, staging_key, ledger_key], args=[source])
            logger.info('Queue migration %s staged %s for transfer to %s', migration_id, source, target)

        moved = _drain_staging_list(
            connection,
            transfer_script,
            staging_key,
            target_key,
            job_class,
            ledger_key,
            lock_token,
            move_mode,
            f'moved:{source}',
        )
        finalize_script(
            keys=[staging_key, source_key, Queue.redis_queues_keys, ledger_key],
            args=[source, source_key, target_key],
        )
        moved_jobs[source] = moved
        logger.info('Queue migration %s finished %s -> %s (%d job(s))', migration_id, source, target, moved)

    return moved_jobs


def _drain_staging_list(
    connection: Redis,
    transfer_script,
    staging_key: str,
    target_key: str,
    job_class: type[Job],
    ledger_key: str,
    lock_token: str,
    move_mode: str,
    moved_field: str,
) -> int:
    """Move all staged job ids to the target in FIFO order, one atomic call each.

    Calls are batched in a non-transactional pipeline for throughput; each
    ``TRANSFER_JOB_SCRIPT`` call is still atomic on its own. After each batch the
    transferred count is accrued in the ledger (a crash between the transfers and
    this bookkeeping can only undercount, never overcount) and the migration
    lock is renewed so a long drain can't be taken over halfway.
    """
    moved = 0
    job_prefix = job_class.redis_job_namespace_prefix
    target_name = target_key[len(Queue.redis_queue_namespace_prefix) :]

    while True:
        with connection.pipeline(transaction=False) as pipeline:
            for _ in range(DRAIN_BATCH_SIZE):
                transfer_script(
                    keys=[staging_key, target_key],
                    args=[job_prefix, target_name, move_mode],
                    client=pipeline,
                )
            results = [int(result) for result in pipeline.execute()]

        batch_moved = sum(results)
        moved += batch_moved
        with connection.pipeline(transaction=False) as pipeline:
            pipeline.hset(ledger_key, 'updated_at', now().isoformat())
            if batch_moved:
                pipeline.hincrby(ledger_key, moved_field, batch_moved)
            pipeline.execute()

        if acquire_or_refresh_lock(connection, MIGRATION_LOCK_KEY, lock_token, MIGRATION_LOCK_TTL) == 'taken':
            raise QueueMigrationInProgressError('Queue migration lock was lost to another process while draining')

        if not results or results[-1] == 0:
            break
    return moved
