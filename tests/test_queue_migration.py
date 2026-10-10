import json

import pytest

from rq import Queue, SimpleWorker, migrate_queues
from rq.exceptions import (
    InvalidQueueMappingError,
    QueueMigrationConflictError,
    QueueMigrationInProgressError,
)
from rq.job import Job, JobStatus
from rq.queue_migration import (
    FINALIZE_SOURCE_SCRIPT,
    LEDGER_KEY_TEMPLATE,
    MIGRATION_KEY_PREFIX,
    MIGRATION_LOCK_KEY,
    STAGE_SOURCE_SCRIPT,
    STAGING_KEY_TEMPLATE,
    TRANSFER_JOB_SCRIPT,
    _migration_id,
)
from rq.registry import StartedJobRegistry
from rq.utils import now
from tests import RQTestCase
from tests.fixtures import say_hello

CONSUMED: list = []


def record(value):
    """Job fixture that records the order in which a worker executes it."""
    CONSUMED.append(value)
    return value


class TestQueueMigration(RQTestCase):
    def _enqueue_jobs(self, queue_name: str, count: int) -> list[str]:
        queue = Queue(queue_name, connection=self.connection)
        return [queue.enqueue(say_hello, name=f'{queue_name}-{i}').id for i in range(count)]

    def _ledger(self, mapping: dict[str, str]):
        migration_id = _migration_id(mapping)
        ledger_key = LEDGER_KEY_TEMPLATE.format(migration_id=migration_id)
        return migration_id, ledger_key

    def _create_ledger(self, mapping: dict[str, str]) -> tuple[str, str]:
        migration_id, ledger_key = self._ledger(mapping)
        self.connection.hset(
            ledger_key,
            mapping={
                'mapping': json.dumps(mapping, sort_keys=True),
                'state': 'in_progress',
                'created_at': now().isoformat(),
                'updated_at': now().isoformat(),
            },
        )
        return migration_id, ledger_key

    def _stage_source(self, migration_id: str, ledger_key: str, source: str) -> str:
        staging_key = STAGING_KEY_TEMPLATE.format(migration_id=migration_id, source=source)
        self.connection.register_script(STAGE_SOURCE_SCRIPT)(
            keys=[f'rq:queue:{source}', staging_key, ledger_key], args=[source]
        )
        return staging_key

    def _migration_keys(self) -> set[bytes]:
        return set(self.connection.scan_iter(f'{MIGRATION_KEY_PREFIX}*'))

    # ------------------------------------------------------------------ happy path

    def test_migrate_preserves_fifo_order_and_origin(self):
        """Pending jobs move in FIFO order and are re-attributed to the new queue."""
        job_ids = self._enqueue_jobs('old', 4)

        result = migrate_queues({'old': 'new'}, self.connection)

        self.assertFalse(result.resumed)
        self.assertEqual(result.moved_jobs, {'old': 4})
        new_queue = Queue('new', connection=self.connection)
        self.assertEqual(new_queue.job_ids, job_ids)
        self.assertEqual(Queue('old', connection=self.connection).count, 0)
        self.assertFalse(self.connection.exists('rq:queue:old'))

        for position, job_id in enumerate(job_ids):
            job = Job.fetch(job_id, connection=self.connection)
            self.assertEqual(job.origin, 'new')
            self.assertEqual(job.get_status(), JobStatus.QUEUED)
            self.assertEqual(new_queue.get_job_position(job_id), position)

        registered = self.connection.smembers('rq:queues')
        self.assertEqual(registered, {b'rq:queue:new'})
        # Lock is released and no staging list is left behind.
        self.assertFalse(self.connection.exists(MIGRATION_LOCK_KEY))
        self.assertFalse(any(b'staging' in key for key in self._migration_keys()))

    def test_merge_two_queues_into_one(self):
        """Two sources merge into one target; each keeps FIFO and sources
        concatenate in lexicographic source-name order."""
        alpha_ids = self._enqueue_jobs('alpha', 3)
        beta_ids = self._enqueue_jobs('beta', 2)

        result = migrate_queues({'alpha': 'merged', 'beta': 'merged'}, self.connection)

        self.assertEqual(result.moved_jobs, {'alpha': 3, 'beta': 2})
        merged = Queue('merged', connection=self.connection)
        self.assertEqual(merged.job_ids, alpha_ids + beta_ids)
        for job_id in alpha_ids + beta_ids:
            self.assertEqual(Job.fetch(job_id, connection=self.connection).origin, 'merged')
        self.assertFalse(self.connection.exists('rq:queue:alpha'))
        self.assertFalse(self.connection.exists('rq:queue:beta'))

    def test_merge_and_rename_in_one_mapping(self):
        """A single mapping can both merge sources and rename others independently."""
        alpha_ids = self._enqueue_jobs('alpha', 2)
        beta_ids = self._enqueue_jobs('beta', 2)
        solo_ids = self._enqueue_jobs('solo', 1)

        migrate_queues({'alpha': 'merged', 'beta': 'merged', 'solo': 'standalone'}, self.connection)

        self.assertEqual(Queue('merged', connection=self.connection).job_ids, alpha_ids + beta_ids)
        self.assertEqual(Queue('standalone', connection=self.connection).job_ids, solo_ids)

    def test_migrated_jobs_are_consumed_from_new_queue(self):
        """A worker that only listens on the new queue drains migrated jobs in order."""
        old_queue = Queue('old', connection=self.connection)
        for i in range(5):
            old_queue.enqueue(record, i)

        migrate_queues({'old': 'new'}, self.connection)

        CONSUMED.clear()
        worker = SimpleWorker([Queue('new', connection=self.connection)], connection=self.connection)
        worker.work(burst=True)

        self.assertEqual(CONSUMED, [0, 1, 2, 3, 4])
        self.assertTrue(Queue('new', connection=self.connection).is_empty())

    def test_empty_and_unknown_source_queues(self):
        """A registered-but-empty queue and a name that never existed migrate cleanly."""
        empty_queue = Queue('empty', connection=self.connection)
        self.connection.sadd('rq:queues', empty_queue.key)

        result = migrate_queues({'empty': 'new', 'ghost': 'new2'}, self.connection)

        self.assertEqual(result.moved_jobs, {'empty': 0, 'ghost': 0})
        registered = self.connection.smembers('rq:queues')
        self.assertEqual(registered, {b'rq:queue:new', b'rq:queue:new2'})
        self.assertFalse(self.connection.exists('rq:queue:empty'))

    def test_late_arrivals_stay_on_old_queue(self):
        """Jobs enqueued after the snapshot stay on the old queue for old workers."""
        job_ids = self._enqueue_jobs('old', 2)
        migrate_queues({'old': 'new'}, self.connection)

        late_job = Queue('old', connection=self.connection).enqueue(say_hello, name='late')

        self.assertEqual(Queue('new', connection=self.connection).job_ids, job_ids)
        self.assertEqual(Queue('old', connection=self.connection).job_ids, [late_job.id])
        self.assertEqual(late_job.origin, 'old')
        registered = self.connection.smembers('rq:queues')
        self.assertIn(b'rq:queue:old', registered)
        self.assertIn(b'rq:queue:new', registered)

    def test_large_queue_is_drained_in_batches_without_reordering(self):
        """Drains larger than a pipeline batch preserve exact FIFO order."""
        job_ids = self._enqueue_jobs('big', 205)
        migrate_queues({'big': 'big2'}, self.connection)
        self.assertEqual(Queue('big2', connection=self.connection).job_ids, job_ids)

    def test_stale_list_entry_is_moved_without_being_resurrected(self):
        """A dangling id whose job hash expired moves as-is but no ghost job hash is created."""
        self.connection.rpush('rq:queue:old', 'deadbeef-stale-id')
        migrate_queues({'old': 'new'}, self.connection)

        self.assertEqual(Queue('new', connection=self.connection).job_ids, ['deadbeef-stale-id'])
        self.assertFalse(self.connection.exists('rq:job:deadbeef-stale-id'))

    # --------------------------------------------------------------- idempotency

    def test_completed_migration_rerun_is_a_noop(self):
        """Rerunning a finished migration neither duplicates nor moves new ids."""
        job_ids = self._enqueue_jobs('old', 3)
        first = migrate_queues({'old': 'new'}, self.connection)
        second = migrate_queues({'old': 'new'}, self.connection)

        self.assertTrue(second.resumed)
        self.assertEqual(second.moved_jobs, {'old': 3})
        self.assertEqual(second.migration_id, first.migration_id)
        self.assertEqual(Queue('new', connection=self.connection).job_ids, job_ids)

    def test_migration_id_is_stable_regardless_of_dict_order(self):
        self.assertEqual(_migration_id({'a': 'x', 'b': 'y'}), _migration_id({'b': 'y', 'a': 'x'}))
        self.assertNotEqual(_migration_id({'a': 'x'}), _migration_id({'a': 'z'}))

    # ------------------------------------------------------------- crash recovery

    def test_resume_after_crash_right_after_staging(self):
        """Crash after RENAME-to-staging but before any transfer resumes fully."""
        job_ids = self._enqueue_jobs('old', 5)
        mapping = {'old': 'new'}
        migration_id, ledger_key = self._create_ledger(mapping)
        self._stage_source(migration_id, ledger_key, 'old')
        # The source list is gone but the jobs are safe in staging.
        self.assertFalse(self.connection.exists('rq:queue:old'))

        result = migrate_queues(mapping, self.connection)

        self.assertTrue(result.resumed)
        self.assertEqual(result.moved_jobs, {'old': 5})
        self.assertEqual(Queue('new', connection=self.connection).job_ids, job_ids)

    def test_resume_after_crash_mid_drain(self):
        """A crash halfway through draining neither duplicates nor loses a job."""
        job_ids = self._enqueue_jobs('old', 10)
        mapping = {'old': 'new'}
        migration_id, ledger_key = self._create_ledger(mapping)
        staging_key = self._stage_source(migration_id, ledger_key, 'old')

        transfer = self.connection.register_script(TRANSFER_JOB_SCRIPT)
        for _ in range(4):  # simulate four transfers committed before the crash
            transfer(keys=[staging_key, 'rq:queue:new'], args=['rq:job:', 'new', 'LR'])

        result = migrate_queues(mapping, self.connection)

        self.assertTrue(result.resumed)
        new_queue = Queue('new', connection=self.connection)
        self.assertEqual(new_queue.count, 10)
        final_ids = new_queue.job_ids
        self.assertEqual(len(set(final_ids)), 10)
        self.assertEqual(final_ids, job_ids)
        self.assertFalse(self.connection.exists(staging_key))

    def test_resume_skips_sources_already_finalized(self):
        """One source finalized before the crash; the other still drains, order intact."""
        alpha_ids = self._enqueue_jobs('alpha', 3)
        beta_ids = self._enqueue_jobs('beta', 2)
        mapping = {'alpha': 'merged', 'beta': 'merged'}
        migration_id, ledger_key = self._create_ledger(mapping)
        staging_key = self._stage_source(migration_id, ledger_key, 'alpha')
        transfer = self.connection.register_script(TRANSFER_JOB_SCRIPT)
        while transfer(keys=[staging_key, 'rq:queue:merged'], args=['rq:job:', 'merged', 'LR']):
            pass
        self.connection.register_script(FINALIZE_SOURCE_SCRIPT)(
            keys=[staging_key, 'rq:queue:alpha', 'rq:queues', ledger_key],
            args=['alpha', 'rq:queue:alpha', 'rq:queue:merged'],
        )

        migrate_queues(mapping, self.connection)

        self.assertEqual(Queue('merged', connection=self.connection).job_ids, alpha_ids + beta_ids)

    # ----------------------------------------------------------------- validation

    def test_invalid_mappings_are_rejected(self):
        invalid_mappings = [
            None,
            {},
            {'old': ''},
            {'': 'new'},
            {42: 'new'},
            {'old': 7},
            {'same': 'same'},
            {'a': 'b', 'b': 'c'},
            {'a': 'b', 'b': 'a'},
        ]
        for mapping in invalid_mappings:
            with self.subTest(mapping=mapping):
                with self.assertRaises(InvalidQueueMappingError):
                    migrate_queues(mapping, self.connection)
                self.assertEqual(self._migration_keys(), set())

    def test_target_conflict_aborts_entire_migration(self):
        """A non-empty target queue aborts everything before a single key is written."""
        first_ids = self._enqueue_jobs('first', 2)
        second_ids = self._enqueue_jobs('second', 2)
        self._enqueue_jobs('busy', 1)

        with pytest.raises(QueueMigrationConflictError, match='busy'):
            migrate_queues({'first': 'busy', 'second': 'busy'}, self.connection)

        # Nothing moved: sources keep their jobs and origins, no ledger/staging/lock.
        self.assertEqual(Queue('first', connection=self.connection).job_ids, first_ids)
        self.assertEqual(Queue('second', connection=self.connection).job_ids, second_ids)
        self.assertEqual(Queue('busy', connection=self.connection).count, 1)
        for job_id in first_ids + second_ids:
            self.assertNotEqual(Job.fetch(job_id, connection=self.connection).origin, 'busy')
        self.assertEqual(self._migration_keys(), set())

    def test_concurrent_migration_is_rejected(self):
        self.connection.set(MIGRATION_LOCK_KEY, 'someone-else', ex=30)
        with pytest.raises(QueueMigrationInProgressError):
            migrate_queues({'old': 'new'}, self.connection)
        # The failed run didn't touch the other owner's lock.
        self.assertEqual(self.connection.get(MIGRATION_LOCK_KEY), b'someone-else')

    # ------------------------------------------------------- untouched neighbouring state

    def test_unmapped_queue_is_untouched(self):
        migrated_ids = self._enqueue_jobs('old', 2)
        other_ids = self._enqueue_jobs('other', 3)

        migrate_queues({'old': 'new'}, self.connection)

        other_queue = Queue('other', connection=self.connection)
        self.assertEqual(other_queue.job_ids, other_ids)
        for job_id in other_ids:
            self.assertEqual(Job.fetch(job_id, connection=self.connection).origin, 'other')
        for job_id in migrated_ids:
            self.assertEqual(Job.fetch(job_id, connection=self.connection).origin, 'new')

    def test_dequeued_and_started_jobs_are_untouched(self):
        """Jobs a live worker already dequeued (intermediate) or is executing
        (started registry) stay put and keep their old origin."""
        queue = Queue('old', connection=self.connection)
        inflight = queue.enqueue(say_hello, name='inflight')
        waiting_ids = [queue.enqueue(say_hello, name=f'w{i}').id for i in range(2)]

        # Mimic a worker dequeue: LMOVE main list -> intermediate list.
        self.connection.lmove(queue.key, queue.intermediate_queue_key, 'LEFT', 'RIGHT')
        # Mimic execution starting: STARTED status plus a started-registry entry.
        inflight.set_status(JobStatus.STARTED)
        score = now().timestamp() + 600
        self.connection.zadd(
            StartedJobRegistry.key_template.format('old'),
            {f'{inflight.id}:execution-1': score},
        )

        migrate_queues({'old': 'new'}, self.connection)

        # Still dequeued/in-flight under the old name.
        self.assertEqual(queue.intermediate_queue.get_job_ids(), [inflight.id])
        self.assertIn(inflight.id, StartedJobRegistry('old', connection=self.connection))
        inflight.refresh()
        self.assertEqual(inflight.origin, 'old')
        self.assertEqual(inflight.get_status(), JobStatus.STARTED)
        self.assertNotIn(inflight.id, Queue('new', connection=self.connection).job_ids)

        # Waiting jobs still moved.
        self.assertEqual(Queue('new', connection=self.connection).job_ids, waiting_ids)
