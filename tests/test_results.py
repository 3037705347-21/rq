import tempfile
import time
from datetime import timedelta

from rq.defaults import UNSERIALIZABLE_RETURN_VALUE_PAYLOAD
from rq.executions import prepare_execution
from rq.job import Job, Retry
from rq.queue import Queue
from rq.registry import StartedJobRegistry
from rq.results import Result, ResultConsumer, ResultStreamGapError, get_key
from rq.utils import now
from rq.worker import Worker
from tests import RQTestCase, min_redis_version

from .fixtures import div_by_zero, say_hello


def add_result(job, value):
    return Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=value)


@min_redis_version((5, 0, 0))
class TestResult(RQTestCase):
    def test_save_and_get_result(self):
        """Ensure data is saved properly"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        result = Result.fetch_latest(job)
        self.assertIsNone(result)

        started = now().replace(microsecond=0)
        ended = started + timedelta(seconds=1)
        Result.create(
            job,
            Result.Type.SUCCESSFUL,
            ttl=10,
            return_value=1,
            worker_name='a',
            execution_id='exec-abc',
            execution_started_at=started,
            execution_ended_at=ended,
        )
        result = Result.fetch_latest(job)
        self.assertEqual(result.return_value, 1)
        self.assertEqual(result.worker_name, 'a')
        self.assertEqual(job.latest_result().return_value, 1)
        self.assertEqual(result.execution_id, 'exec-abc')
        self.assertEqual(result.execution_started_at, started)
        self.assertEqual(result.execution_ended_at, ended)

        # Check that ttl is properly set
        key = get_key(job.id)
        ttl = self.connection.pttl(key)
        self.assertTrue(5000 < ttl <= 10000)

        # Check job with None return value
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=None)
        result = Result.fetch_latest(job)
        self.assertIsNone(result.return_value)
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=2)
        result = Result.fetch_latest(job)
        self.assertEqual(result.return_value, 2)

    def test_execution_info_backwards_compatible(self):
        """Results without execution info restore with None fields (old data compat)."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        # Create a result without any execution info (simulates pre-upgrade data).
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1, worker_name='a')
        result = Result.fetch_latest(job)
        self.assertIsNone(result.execution_id)
        self.assertIsNone(result.execution_started_at)
        self.assertIsNone(result.execution_ended_at)

        # Same for failure results.
        Result.create_failure(job, ttl=10, exc_string='boom', worker_name='a')
        result = Result.fetch_latest(job)
        self.assertIsNone(result.execution_id)
        self.assertIsNone(result.execution_started_at)
        self.assertIsNone(result.execution_ended_at)

    def test_create_failure(self):
        """Ensure data is saved properly"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        Result.create_failure(job, ttl=10, exc_string='exception', worker_name='a')
        result = Result.fetch_latest(job)
        self.assertEqual(result.worker_name, 'a')
        self.assertEqual(result.exc_string, 'exception')

        # Check that ttl is properly set
        key = get_key(job.id)
        ttl = self.connection.pttl(key)
        self.assertTrue(5000 < ttl <= 10000)

    def test_create_retried(self):
        """Ensure retried result preserves returned Retry object"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        retry = Retry(max=1)

        started = now()
        ended = started + timedelta(seconds=1)
        Result.create_retried(
            job,
            ttl=10,
            return_value=retry,
            worker_name='a',
            execution_id='exec-1',
            execution_started_at=started,
            execution_ended_at=ended,
        )
        result = Result.fetch_latest(job)
        self.assertEqual(result.type, Result.Type.RETRIED)
        self.assertEqual(result.worker_name, 'a')
        self.assertEqual(result.execution_id, 'exec-1')
        self.assertIsInstance(result.return_value, Retry)
        self.assertEqual(result.return_value.max, retry.max)
        self.assertEqual(result.return_value.intervals, retry.intervals)

    def test_create_max_retries_exceeded(self):
        """Ensure max retries exceeded result preserves returned Retry object"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        retry = Retry(max=1)

        started = now()
        ended = started + timedelta(seconds=1)
        Result.create_max_retries_exceeded(
            job,
            ttl=10,
            return_value=retry,
            worker_name='a',
            execution_id='exec-2',
            execution_started_at=started,
            execution_ended_at=ended,
        )
        result = Result.fetch_latest(job)
        self.assertEqual(result.type, Result.Type.MAX_RETRIES_EXCEEDED)
        self.assertEqual(result.execution_id, 'exec-2')
        self.assertEqual(result.worker_name, 'a')
        self.assertIsNone(result.exc_string)
        self.assertIsInstance(result.return_value, Retry)
        self.assertEqual(result.return_value.max, retry.max)
        self.assertEqual(result.return_value.intervals, retry.intervals)

    def test_getting_results(self):
        """Check getting all execution results"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        # latest_result() returns None when there's no result
        self.assertIsNone(job.latest_result())

        result_1 = Result.create_failure(job, ttl=10, exc_string='exception')
        result_2 = Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)
        result_3 = Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)

        # Result.fetch_latest() returns the latest result
        result = Result.fetch_latest(job)
        self.assertEqual(result, result_3)
        self.assertEqual(job.latest_result(), result_3)

        # Result.all() and job.results() returns all results, newest first
        results = Result.all(job)
        self.assertEqual(results, [result_3, result_2, result_1])
        self.assertEqual(job.results(), [result_3, result_2, result_1])

    def test_count(self):
        """Result.count(job) returns number of results"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        self.assertEqual(Result.count(job), 0)
        Result.create_failure(job, ttl=10, exc_string='exception')
        self.assertEqual(Result.count(job), 1)
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)
        self.assertEqual(Result.count(job), 2)

    def test_delete_all(self):
        """Result.delete_all(job) deletes all results from Redis"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        Result.create_failure(job, ttl=10, exc_string='exception')
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)
        Result.delete_all(job)
        self.assertEqual(Result.count(job), 0)

    def test_job_successful_result(self):
        """Test job successful result handling."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        worker = Worker([queue], connection=self.connection)
        worker.register_birth()

        self.assertEqual(worker.failed_job_count, 0)
        self.assertEqual(worker.successful_job_count, 0)
        self.assertEqual(worker.total_working_time, 0)

        # These should only run on workers that supports Redis streams
        registry = StartedJobRegistry(connection=self.connection)
        job.started_at = now()
        job.ended_at = job.started_at + timedelta(seconds=0.75)
        job._result = 'Success'
        execution = prepare_execution(worker, job)
        worker.handle_job_success(job, queue, registry, execution)

        payload = self.connection.hgetall(job.key)
        self.assertNotIn(b'result', payload.keys())
        self.assertEqual(job.result, 'Success')

        # Result carries execution metadata populated by the worker.
        result = job.latest_result()
        self.assertEqual(result.execution_id, execution.id)
        self.assertEqual(result.execution_ended_at, job.ended_at)
        self.assertIsNotNone(result.execution_started_at)

    def test_job_failed_result(self):
        """Test job failure result handling."""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        worker = Worker([queue], connection=self.connection)
        worker.register_birth()

        self.assertEqual(worker.failed_job_count, 0)
        self.assertEqual(worker.successful_job_count, 0)
        self.assertEqual(worker.total_working_time, 0)

        registry = StartedJobRegistry(connection=self.connection)
        job.started_at = now()
        job.ended_at = job.started_at + timedelta(seconds=0.75)
        execution = prepare_execution(worker, job)
        worker.handle_job_failure(
            job, exc_string='Error', queue=queue, started_job_registry=registry, execution=execution
        )

        job = Job.fetch(job.id, connection=self.connection)
        payload = self.connection.hgetall(job.key)
        self.assertNotIn(b'exc_info', payload.keys())
        self.assertEqual(job.exc_info, 'Error')

        # Result carries execution metadata populated by the worker.
        result = job.latest_result()
        self.assertEqual(result.execution_id, execution.id)
        self.assertEqual(result.execution_ended_at, job.ended_at)
        self.assertIsNotNone(result.execution_started_at)

    def test_job_return_value(self):
        """Test job.return_value"""
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        # Returns None when there's no result
        self.assertIsNone(job.return_value())

        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)
        self.assertEqual(job.return_value(), 1)

        # Returns None if latest result is a failure
        Result.create_failure(job, ttl=10, exc_string='exception')
        self.assertIsNone(job.return_value(refresh=True))

    def test_job_return_value_sync(self):
        """Test job.return_value when queue.is_async=False"""
        queue = Queue(connection=self.connection, is_async=False)
        job = queue.enqueue(say_hello)

        # Returns None when there's no result
        self.assertIsNotNone(job.return_value())

        job = queue.enqueue(div_by_zero)
        self.assertEqual(job.latest_result().type, Result.Type.FAILED)

    def test_job_return_value_result_ttl_infinity(self):
        """Test job.return_value when queue.result_ttl=-1"""
        queue = Queue(connection=self.connection, result_ttl=-1)
        job = queue.enqueue(say_hello)

        # Returns None when there's no result
        self.assertIsNone(job.return_value())

        Result.create(job, Result.Type.SUCCESSFUL, ttl=-1, return_value=1)
        self.assertEqual(job.return_value(), 1)

    def test_job_return_value_result_ttl_zero(self):
        """Test job.return_value when queue.result_ttl=0"""
        queue = Queue(connection=self.connection, result_ttl=0)
        job = queue.enqueue(say_hello)

        # Returns None when there's no result
        self.assertIsNone(job.return_value())

        Result.create(job, Result.Type.SUCCESSFUL, ttl=0, return_value=1)
        self.assertIsNone(job.return_value())

    def test_job_return_value_unserializable(self):
        """Test job.return_value when it is not serializable"""
        queue = Queue(connection=self.connection, result_ttl=0)
        job = queue.enqueue(say_hello)

        # Returns None when there's no result
        self.assertIsNone(job.return_value())

        # tempfile.NamedTemporaryFile() is not picklable
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=tempfile.NamedTemporaryFile())
        self.assertEqual(job.return_value(), UNSERIALIZABLE_RETURN_VALUE_PAYLOAD)
        self.assertEqual(Result.count(job), 1)

        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)
        self.assertEqual(Result.count(job), 2)

    def test_blocking_results(self):
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)

        # Should block if there's no result.
        timeout = 1
        self.assertIsNone(Result.fetch_latest(job))
        started_at = time.time()
        self.assertIsNone(Result.fetch_latest(job, timeout=timeout))
        blocked_for = time.time() - started_at
        self.assertGreaterEqual(blocked_for, timeout)

        # Shouldn't block if there's already a result present.
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=1)
        timeout = 1
        result_sync = Result.fetch_latest(job)
        started_at = time.time()
        result_blocking = Result.fetch_latest(job, timeout=timeout)
        blocked_for = time.time() - started_at
        self.assertEqual(result_sync.return_value, result_blocking.return_value)
        self.assertGreater(timeout, blocked_for)

        # Should return the latest result if there are multiple.
        Result.create(job, Result.Type.SUCCESSFUL, ttl=10, return_value=2)
        result_blocking = Result.fetch_latest(job, timeout=1)
        self.assertEqual(result_blocking.return_value, 2)


@min_redis_version((5, 0, 0))
class TestResultConsumer(RQTestCase):
    def create_job(self):
        queue = Queue(connection=self.connection)
        return queue.enqueue(say_hello)

    def test_requires_name(self):
        """Consumer name must be a non-empty string"""
        job = self.create_job()
        with self.assertRaises(ValueError):
            ResultConsumer(job, '')
        with self.assertRaises(ValueError):
            ResultConsumer(job, None)

    def test_new_consumer_reads_from_beginning(self):
        """Consumer without a checkpoint reads all existing results, oldest first"""
        job = self.create_job()
        result_1 = add_result(job, 1)
        result_2 = add_result(job, 2)

        consumer = ResultConsumer(job, 'alice')
        self.assertIsNone(consumer.position())
        results = consumer.read()
        self.assertEqual(results, [result_1, result_2])
        # Reading must not advance the checkpoint.
        self.assertIsNone(consumer.position())

    def test_read_empty_stream(self):
        """Reading a stream with no results returns an empty list"""
        job = self.create_job()
        consumer = ResultConsumer(job, 'alice')
        self.assertEqual(consumer.read(), [])

    def test_incremental_read_with_count_and_ack(self):
        """After ack, only newer results are delivered, in batches if requested"""
        job = self.create_job()
        result_1 = add_result(job, 1)
        result_2 = add_result(job, 2)

        consumer = ResultConsumer(job, 'alice')
        batch = consumer.read(count=1)
        self.assertEqual(batch, [result_1])
        self.assertEqual(consumer.ack(), result_1.id)
        self.assertEqual(consumer.position(), result_1.id)

        batch = consumer.read(count=1)
        self.assertEqual(batch, [result_2])
        consumer.ack()

        # Everything processed: nothing new, and a second read delivers nothing.
        self.assertEqual(consumer.read(), [])
        add_result(job, 3)
        self.assertEqual(len(consumer.read()), 1)

    def test_checkpoint_survives_consumer_restart(self):
        """A new consumer instance with the same name resumes at the checkpoint"""
        job = self.create_job()
        result_1 = add_result(job, 1)
        add_result(job, 2)

        consumer = ResultConsumer(job, 'alice')
        self.assertEqual(len(consumer.read(count=1)), 1)
        consumer.ack()

        # Simulate a process restart: brand new instance, same consumer name.
        restarted = ResultConsumer(job, 'alice')
        self.assertEqual(restarted.position(), result_1.id)
        results = restarted.read()
        self.assertEqual([result.return_value for result in results], [2])

    def test_consumers_are_independent(self):
        """Checkpoints of different consumers never affect each other"""
        job = self.create_job()
        result_1 = add_result(job, 1)
        result_2 = add_result(job, 2)

        alice = ResultConsumer(job, 'alice')
        bob = ResultConsumer(job, 'bob')

        self.assertEqual(alice.read(), [result_1, result_2])
        alice.ack(result_2.id)
        self.assertEqual(alice.read(), [])

        # Bob has no checkpoint and still sees everything.
        self.assertEqual(bob.read(), [result_1, result_2])
        bob.ack(result_1.id)

        # Alice stays at the tip regardless of Bob's move.
        self.assertEqual(alice.position(), result_2.id)
        self.assertEqual(bob.position(), result_1.id)
        self.assertEqual(bob.read(), [result_2])

    def test_failure_before_ack_redelivers_and_converges(self):
        """Crash/failure before ack redelivers; reprocessing converges to one checkpoint"""
        job = self.create_job()
        result_1 = add_result(job, 1)
        result_2 = add_result(job, 2)

        consumer = ResultConsumer(job, 'alice')
        self.assertEqual(consumer.read(), [result_1, result_2])
        # Processing fails / process dies: no ack() happened.
        self.assertIsNone(consumer.position())

        # Restart and reprocess the exact same batch.
        restarted = ResultConsumer(job, 'alice')
        redelivered = restarted.read()
        self.assertEqual(redelivered, [result_1, result_2])
        restarted.ack(result_2.id)

        self.assertEqual(restarted.position(), result_2.id)
        self.assertEqual(restarted.read(), [])
        # Acking the same position again is a harmless no-op convergence.
        restarted.ack(result_2.id)
        self.assertEqual(restarted.position(), result_2.id)

    def test_ack_validation(self):
        """ack needs processed results and can never move the checkpoint backwards"""
        job = self.create_job()
        result_1 = add_result(job, 1)
        result_2 = add_result(job, 2)

        consumer = ResultConsumer(job, 'alice')
        with self.assertRaises(ValueError):
            consumer.ack()

        consumer.read()
        consumer.ack(result_2.id)
        with self.assertRaises(ValueError):
            consumer.ack(result_1.id)

    def test_seek_moves_checkpoint_forward(self):
        """seek() explicitly jumps over results (gap recovery), never backwards"""
        job = self.create_job()
        result_1 = add_result(job, 1)
        result_2 = add_result(job, 2)

        consumer = ResultConsumer(job, 'alice')
        consumer.read()
        consumer.seek(result_1.id)
        self.assertEqual(consumer.position(), result_1.id)
        self.assertEqual(consumer.read(), [result_2])

        consumer.seek(result_2.id)
        self.assertEqual(consumer.read(), [])
        with self.assertRaises(ValueError):
            consumer.seek(result_1.id)

    def test_gap_raised_when_entries_trimmed(self):
        """Trimmed entries past the checkpoint raise instead of being skipped"""
        job = self.create_job()
        result_1 = add_result(job, 1)

        consumer = ResultConsumer(job, 'alice')
        consumer.read(count=1)
        consumer.ack()

        # Retention trims the acknowledged entry; a new, undelivered entry remains.
        stream_key = get_key(job.id)
        surviving_id = self.connection.xadd(stream_key, {'type': '1'}, maxlen=1, approximate=False).decode()
        self.assertNotEqual(surviving_id, result_1.id)

        with self.assertRaises(ResultStreamGapError) as ctx:
            consumer.read()
        gap = ctx.exception
        self.assertEqual(gap.last_id, result_1.id)
        self.assertEqual(gap.first_available_id, surviving_id)
        # Failed gap check must not touch the checkpoint.
        self.assertEqual(consumer.position(), result_1.id)

        # A consumer without a checkpoint sees no gap (nothing promised yet).
        newcomer = ResultConsumer(job, 'newcomer')
        self.assertEqual([r.id for r in newcomer.read()], [surviving_id])

        # Explicit recovery: reset replays from the oldest surviving entry.
        consumer.reset()
        self.assertIsNone(consumer.position())
        self.assertEqual([r.id for r in consumer.read()], [surviving_id])

    def test_gap_recovery_with_seek(self):
        """seek() to the first surviving entry resumes past the gap"""
        job = self.create_job()
        add_result(job, 1)
        consumer = ResultConsumer(job, 'alice')
        consumer.read(count=1)
        consumer.ack()

        stream_key = get_key(job.id)
        surviving_id = self.connection.xadd(stream_key, {'type': '1'}, maxlen=1, approximate=False).decode()
        with self.assertRaises(ResultStreamGapError):
            consumer.read()

        consumer.seek(surviving_id)
        self.assertEqual(consumer.position(), surviving_id)
        self.assertEqual(consumer.read(), [])

    def test_gap_raised_when_stream_recreated(self):
        """A stream deleted/expired after the checkpoint and recreated is a gap"""
        job = self.create_job()
        result_1 = add_result(job, 1)
        consumer = ResultConsumer(job, 'alice')
        consumer.read(count=1)
        consumer.ack()

        stream_key = get_key(job.id)
        self.connection.delete(stream_key)
        # New entries use a strictly greater (explicit) stream ID.
        milliseconds = int(result_1.id.split('-')[0])
        recreated_id = f'{milliseconds + 10}-0'
        self.connection.xadd(stream_key, {'type': '1'}, id=recreated_id)

        with self.assertRaises(ResultStreamGapError) as ctx:
            consumer.read()
        self.assertEqual(ctx.exception.last_id, result_1.id)
        self.assertEqual(ctx.exception.first_available_id, recreated_id)

    def test_blocking_read(self):
        """block waits for new results up to a timeout"""
        job = self.create_job()
        consumer = ResultConsumer(job, 'alice')

        started_at = time.time()
        self.assertEqual(consumer.read(block=1), [])
        self.assertGreaterEqual(time.time() - started_at, 1)

        result_1 = add_result(job, 1)
        started_at = time.time()
        results = consumer.read(block=2)
        self.assertEqual(results, [result_1])
        self.assertLess(time.time() - started_at, 1)

    def test_checkpoint_ttl_mirrors_stream(self):
        """Checkpoint keys inherit the stream TTL and persist when the stream persists"""
        job = self.create_job()
        add_result(job, 1)
        positions_key = ResultConsumer.get_key(job.id)

        consumer = ResultConsumer(job, 'alice')
        consumer.read()
        consumer.ack()
        self.assertTrue(0 < self.connection.pttl(positions_key) <= 10000)

        # A stream kept forever keeps its checkpoints forever too.
        Result.create(job, Result.Type.SUCCESSFUL, ttl=-1, return_value=2)
        persistent = ResultConsumer(job, 'bob')
        persistent.read()
        persistent.ack()
        self.assertEqual(self.connection.pttl(positions_key), -1)

    def test_delete_all_removes_checkpoints(self):
        """Result.delete_all() also removes consumer positions"""
        job = self.create_job()
        add_result(job, 1)
        consumer = ResultConsumer(job, 'alice')
        consumer.read()
        consumer.ack()

        Result.delete_all(job)
        self.assertFalse(self.connection.exists(ResultConsumer.get_key(job.id)))
        self.assertIsNone(ResultConsumer(job, 'alice').position())

    def test_job_delete_removes_checkpoints(self):
        """Job.delete() also removes consumer positions"""
        job = self.create_job()
        add_result(job, 1)
        consumer = ResultConsumer(job, 'alice')
        consumer.read()
        consumer.ack()

        job.delete()
        self.assertFalse(self.connection.exists(ResultConsumer.get_key(job.id)))
