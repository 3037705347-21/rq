"""Tests for content-store offloading of large job payloads.

Covers:
- content-addressed dedup and strict marker codec;
- transparent, byte-identical reads after migration (incl. JSON serializer);
- resumable / idempotent migration;
- no dangling references when garbage collection races the swap;
- GC never reaping payloads of queued/started (or any live) jobs;
- explicit errors that leave the job untouched when a payload is unreadable,
  missing or corrupted;
- migrated jobs still executing correctly under a real worker.
"""

import os
from unittest.mock import patch

from redis import Redis, WatchError
from redis.client import Pipeline

from rq import Queue, SimpleWorker
from rq.exceptions import (
    PayloadIntegrityError,
    PayloadNotFoundError,
    PayloadUnreadableError,
)
from rq.executions import Execution
from rq.job import Job, JobStatus
from rq.payload_migration import (
    DEFAULT_MIGRATION_STATE_KEY,
    PayloadMigrator,
    migrate_large_payloads,
)
from rq.payload_store import (
    BLOB_INDEX_KEY,
    PAYLOAD_MARKER_PREFIX,
    PayloadRef,
    PayloadStore,
    is_payload_marker,
)
from rq.serializers import JSONSerializer
from tests import RQTestCase, fixtures

MIN_SIZE = 1024


def big_payload(kib: int = 20) -> str:
    """Incompressible, JSON- and pickle-safe string large enough to be offloaded."""
    return os.urandom(kib * 1024).hex()


class TestPayloadStore(RQTestCase):
    def test_marker_codec(self):
        ref = PayloadRef('sha256', 'a' * 64)
        marker = ref.to_marker()
        assert marker == PAYLOAD_MARKER_PREFIX + b'sha256:' + b'a' * 64
        assert PayloadRef.from_marker(marker) == ref
        assert is_payload_marker(marker)

    def test_malformed_markers(self):
        for bad in (
            b'rq-payload:v1:sha256:' + b'a' * 63,  # wrong digest length
            b'rq-payload:v1:md5:' + b'a' * 32,  # unsupported algorithm
            b'rq-payload:v1:sha256:zz',  # non-hex
            b'rq-payload:v2:sha256:' + b'a' * 64,  # wrong version
            b'\x78\x9cwhatever',  # zlib stream, not a marker
        ):
            with self.assertRaises(PayloadIntegrityError):
                PayloadRef.from_marker(bad)
            assert not is_payload_marker(bad) or bad.startswith(PAYLOAD_MARKER_PREFIX)

    def test_put_is_content_addressed_and_deduplicated(self):
        store = PayloadStore(self.connection)
        data = os.urandom(4096)
        ref1 = store.put(data)
        ref2 = store.put(data)
        assert ref1 == ref2
        assert self.connection.get(ref1.blob_key) == data
        assert self.connection.sismember(BLOB_INDEX_KEY, ref1.token)
        # Only one blob key exists for the shared content.
        assert self.connection.scard(BLOB_INDEX_KEY) == 1

    def test_get_missing_and_corrupt(self):
        store = PayloadStore(self.connection)
        ref = store.put(b'hello')
        with self.assertRaises(PayloadNotFoundError):
            store.get(PayloadRef('sha256', '0' * 64))
        self.connection.set(ref.blob_key, b'corrupted')
        with self.assertRaises(PayloadIntegrityError):
            store.get(ref)

    def test_is_job_key_excludes_auxiliary_keys(self):
        from rq.payload_store import is_job_key

        assert is_job_key('rq:job:abc-123_XYZ')
        assert not is_job_key('rq:job:abc:dependents')
        assert not is_job_key('rq:job::abc:dependencies')
        assert not is_job_key('rq:queue:default')


class TestPayloadMigration(RQTestCase):
    def enqueue_big(self, queue: Queue, payload: str) -> Job:
        return queue.enqueue(fixtures.say_hello, payload)

    def test_migrates_large_and_skips_small(self):
        queue = Queue(connection=self.connection)
        big = self.enqueue_big(queue, big_payload())
        small = queue.enqueue(fixtures.say_hello, 'tiny')

        report = migrate_large_payloads(self.connection, min_size=MIN_SIZE)

        assert report.complete is True
        assert report.scanned == 2
        assert report.migrated == 1
        assert report.skipped_small == 1
        assert is_payload_marker(self.connection.hget(big.key, 'data'))
        assert not is_payload_marker(self.connection.hget(small.key, 'data'))

    def test_migrated_job_reads_identically(self):
        queue = Queue(connection=self.connection)
        payload = big_payload()
        job = self.enqueue_big(queue, payload)
        expected = {
            'func_name': job.func_name,
            'args': job.args,
            'kwargs': job.kwargs,
            'description': job.description,
        }

        migrate_large_payloads(self.connection, min_size=MIN_SIZE)
        fetched = Job.fetch(job.id, connection=self.connection)

        assert fetched.func_name == expected['func_name']
        assert fetched.args == (payload,)
        assert fetched.kwargs == expected['kwargs']
        assert fetched.description == expected['description']
        # Other hash fields survive untouched.
        assert fetched.origin == queue.name
        assert fetched.get_status() == JobStatus.QUEUED

    def test_identical_payloads_share_one_blob(self):
        queue = Queue(connection=self.connection)
        payload = big_payload()
        first = self.enqueue_big(queue, payload)
        second = self.enqueue_big(queue, payload)

        report = migrate_large_payloads(self.connection, min_size=MIN_SIZE)

        assert report.migrated == 2
        assert report.deduplicated == 1
        assert self.connection.scard(BLOB_INDEX_KEY) == 1
        ref = PayloadRef.from_marker(self.connection.hget(first.key, 'data'))
        assert self.connection.hget(second.key, 'data') == ref.to_marker()

    def test_resumes_from_checkpoint_and_is_idempotent(self):
        queue = Queue(connection=self.connection)
        jobs = [self.enqueue_big(queue, big_payload(2)) for _ in range(6)]

        # SCAN COUNT is only a hint; the processed-id set keeps resume exact even
        # if the whole keyspace fit in the first page (cursor already back to 0).
        first = PayloadMigrator(self.connection, min_size=MIN_SIZE, batch_size=2).migrate(limit=2)
        assert first.complete is False
        assert first.migrated == 2
        state = self.connection.hgetall(DEFAULT_MIGRATION_STATE_KEY)
        assert b'finished_at' not in state and b'started_at' in state

        second = PayloadMigrator(self.connection, min_size=MIN_SIZE, batch_size=2).migrate()
        assert second.resumed is True
        assert second.complete is True
        assert second.migrated == 6
        assert second.scanned == 6
        assert all(is_payload_marker(self.connection.hget(job.key, 'data')) for job in jobs)

        # A full rerun performs no new migrations and finds every reference intact.
        rerun = migrate_large_payloads(self.connection, min_size=MIN_SIZE, restart=True)
        assert rerun.migrated == 0
        assert rerun.already_offloaded == 6
        assert rerun.failures == []

    def test_unreadable_inline_payload_is_reported_and_left_untouched(self):
        queue = Queue(connection=self.connection)
        good = self.enqueue_big(queue, big_payload())
        bad = Job.create(fixtures.say_hello, args=('bad',), connection=self.connection)
        bad.origin = queue.name
        bad.save()
        garbage = b'\x00\x01not-zlib-or-pickle' + b'g' * MIN_SIZE
        self.connection.hset(bad.key, 'data', garbage)

        report = migrate_large_payloads(self.connection, min_size=MIN_SIZE)

        assert is_payload_marker(self.connection.hget(good.key, 'data'))
        assert self.connection.hget(bad.key, 'data') == garbage
        failure = next(f for f in report.failures if f.job_id == bad.id)
        assert failure.error_type == 'PayloadUnreadableError'
        with self.assertRaises(PayloadUnreadableError):
            PayloadMigrator(self.connection)._ensure_readable(garbage)

    def test_missing_blob_is_reported_and_marker_left_untouched(self):
        queue = Queue(connection=self.connection)
        job = self.enqueue_big(queue, big_payload())
        migrate_large_payloads(self.connection, min_size=MIN_SIZE)

        marker = self.connection.hget(job.key, 'data')
        ref = PayloadRef.from_marker(marker)
        self.connection.delete(ref.blob_key)

        with self.assertRaises(PayloadNotFoundError):
            Job.fetch(job.id, connection=self.connection)

        report = migrate_large_payloads(self.connection, min_size=MIN_SIZE, restart=True)
        failure = next(f for f in report.failures if f.job_id == job.id)
        assert failure.error_type == 'PayloadNotFoundError'
        assert self.connection.hget(job.key, 'data') == marker

    def test_corrupt_blob_is_reported_and_marker_left_untouched(self):
        queue = Queue(connection=self.connection)
        job = self.enqueue_big(queue, big_payload())
        migrate_large_payloads(self.connection, min_size=MIN_SIZE)

        marker = self.connection.hget(job.key, 'data')
        ref = PayloadRef.from_marker(marker)
        self.connection.set(ref.blob_key, b'corrupted-bytes')

        with self.assertRaises(PayloadIntegrityError):
            Job.fetch(job.id, connection=self.connection)

        report = migrate_large_payloads(self.connection, min_size=MIN_SIZE, restart=True)
        failure = next(f for f in report.failures if f.job_id == job.id)
        assert failure.error_type == 'PayloadIntegrityError'
        assert self.connection.hget(job.key, 'data') == marker

    def test_malformed_marker_raises_on_fetch(self):
        queue = Queue(connection=self.connection)
        job = queue.enqueue(fixtures.say_hello, 'x')
        self.connection.hset(job.key, 'data', b'rq-payload:v1:sha256:zz')
        with self.assertRaises(PayloadIntegrityError):
            Job.fetch(job.id, connection=self.connection)

    def test_swap_retries_when_gc_deletes_blob_before_commit(self):
        # The blob disappears after WATCH but before EXEC: the swap aborts, the blob
        # is re-published and the retry commits; the job never holds a dangling ref.
        queue = Queue(connection=self.connection)
        job = self.enqueue_big(queue, big_payload())
        original = self.connection.hget(job.key, 'data')
        migrator = PayloadMigrator(self.connection, min_size=MIN_SIZE)

        # Pre-publish the blob (as the migrator would) and delete it from another
        # connection the first time the swap transaction executes.
        ref = migrator.store.put(original)
        other = Redis()
        armed = {'on': True}
        real_execute = Pipeline.execute

        def racy_execute(self, *args, **kwargs):
            if armed['on'] and any(command[0] in (b'HSET', 'HSET') for command in self.command_stack):
                armed['on'] = False
                other.delete(ref.blob_key)
                raise WatchError('simulated GC delete during swap')
            return real_execute(self, *args, **kwargs)

        with patch.object(Pipeline, 'execute', racy_execute):
            report = migrator.migrate(restart=True)
        other.close()

        assert report.failures == []
        assert is_payload_marker(self.connection.hget(job.key, 'data'))
        # The retry re-published the blob; the reference resolves correctly.
        assert Job.fetch(job.id, connection=self.connection).args == job.args

    def test_orphan_blob_deleted_before_swap_is_recreated(self):
        # Crash window: the blob was uploaded but the swap never happened. GC removes
        # the orphan; a later migration run simply re-uploads and succeeds.
        queue = Queue(connection=self.connection)
        job = self.enqueue_big(queue, big_payload())
        raw = self.connection.hget(job.key, 'data')
        ref = PayloadStore(self.connection).put(raw)

        report_gc = PayloadStore(self.connection).garbage_collect()
        assert report_gc.removed_blobs == 1
        assert not self.connection.exists(ref.blob_key)

        report = migrate_large_payloads(self.connection, min_size=MIN_SIZE, restart=True)
        assert report.migrated == 1
        assert Job.fetch(job.id, connection=self.connection).args == job.args

    def test_migrated_job_still_runs_under_a_worker(self):
        queue = Queue('offloaded', connection=self.connection)
        job = self.enqueue_big(queue, big_payload())
        migrate_large_payloads(self.connection, min_size=MIN_SIZE)
        assert is_payload_marker(self.connection.hget(job.key, 'data'))

        worker = SimpleWorker([queue], connection=self.connection)
        worker.work(burst=True)

        assert job.get_status() == JobStatus.FINISHED
        assert job.return_value(refresh=True).startswith('Hi there, ')

    def test_json_serialized_jobs(self):
        queue = Queue('json', connection=self.connection, serializer=JSONSerializer)
        payload = big_payload()
        job = queue.enqueue(fixtures.say_hello, payload)

        # The default pickle reader must not "validate" a JSON job; pass its serializer.
        report = PayloadMigrator(self.connection, serializer=JSONSerializer, min_size=MIN_SIZE).migrate()
        assert report.migrated == 1 and report.failures == []
        assert is_payload_marker(self.connection.hget(job.key, 'data'))

        fetched = Job.fetch(job.id, connection=self.connection, serializer=JSONSerializer)
        assert fetched.args == [payload]

    def test_saving_migrated_job_reinlines_payload(self):
        queue = Queue(connection=self.connection)
        job = self.enqueue_big(queue, big_payload())
        migrate_large_payloads(self.connection, min_size=MIN_SIZE)
        marker = self.connection.hget(job.key, 'data')

        fetched = Job.fetch(job.id, connection=self.connection)
        fetched.save()  # ordinary RQ write path (requeue/retry/failure handling)

        rewritten = self.connection.hget(job.key, 'data')
        assert rewritten != marker
        assert not is_payload_marker(rewritten)
        assert Job.fetch(job.id, connection=self.connection).args == job.args
        # The old blob is now unreferenced and can be garbage collected.
        gc = PayloadStore(self.connection).garbage_collect()
        assert gc.removed_blobs == 1

    def test_auxiliary_job_keys_are_not_processed(self):
        queue = Queue(connection=self.connection)
        dependency = queue.enqueue(fixtures.do_nothing)
        dependent = queue.enqueue(fixtures.say_hello, 'x', depends_on=dependency)

        report = migrate_large_payloads(self.connection, min_size=MIN_SIZE)
        # Only the two job hashes, never rq:job:<id>:dependents / ::dependencies.
        assert report.scanned == 2
        assert Job.fetch(dependent.id, connection=self.connection).kwargs == {}


class TestPayloadGarbageCollection(RQTestCase):
    def test_queued_and_started_jobs_keep_their_blobs(self):
        store = PayloadStore(self.connection)
        queue = Queue('gc', connection=self.connection)

        queued = queue.enqueue(fixtures.say_hello, big_payload())
        started = queue.enqueue(fixtures.say_hello, big_payload(2))
        migrate_large_payloads(self.connection, min_size=MIN_SIZE)
        queued_ref = PayloadRef.from_marker(self.connection.hget(queued.key, 'data'))
        started_ref = PayloadRef.from_marker(self.connection.hget(started.key, 'data'))

        # Move `started` into the StartedJobRegistry, as a worker would.
        started.set_status(JobStatus.STARTED)
        with self.connection.pipeline() as pipe:
            Execution.create(started, ttl=600, pipeline=pipe, worker_name='test-worker')
            pipe.execute()

        gc = store.garbage_collect()
        assert gc.removed_blobs == 0
        assert self.connection.exists(queued_ref.blob_key)
        assert self.connection.exists(started_ref.blob_key)

        # Queue drains (job hash deleted): the queued blob is swept; the started one stays.
        queued.delete()
        gc = store.garbage_collect()
        assert not self.connection.exists(queued_ref.blob_key)
        assert self.connection.exists(started_ref.blob_key)

        # Execution ends and its job is removed: the last reference goes away.
        started.delete()
        gc = store.garbage_collect()
        assert gc.removed_blobs >= 1
        assert not self.connection.exists(started_ref.blob_key)
        assert self.connection.scard(BLOB_INDEX_KEY) == 0

    def test_shared_blob_lives_until_last_reference_is_gone(self):
        store = PayloadStore(self.connection)
        queue = Queue(connection=self.connection)
        payload = big_payload()
        first = queue.enqueue(fixtures.say_hello, payload)
        second = queue.enqueue(fixtures.say_hello, payload)
        migrate_large_payloads(self.connection, min_size=MIN_SIZE)
        ref = PayloadRef.from_marker(self.connection.hget(first.key, 'data'))

        first.delete()
        gc = store.garbage_collect()
        assert gc.removed_blobs == 0
        assert self.connection.exists(ref.blob_key)
        # The surviving job still reads identically.
        assert Job.fetch(second.id, connection=self.connection).args == (payload,)

        second.delete()
        gc = store.garbage_collect()
        assert gc.removed_blobs == 1
        assert not self.connection.exists(ref.blob_key)

    def test_orphan_blob_is_collected(self):
        store = PayloadStore(self.connection)
        ref = store.put(os.urandom(8192))  # uploaded, never referenced by a job
        gc = store.garbage_collect()
        assert gc.removed_blobs == 1
        assert not self.connection.exists(ref.blob_key)
        assert not self.connection.sismember(BLOB_INDEX_KEY, ref.token)
