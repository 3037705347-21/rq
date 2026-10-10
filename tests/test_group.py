from time import sleep

import pytest

from rq import Queue, SimpleWorker
from rq.exceptions import InvalidRosterError, NoSuchGroupError
from rq.group import Group, GroupResult
from rq.job import Job
from rq.results import Result
from rq.utils import as_text
from tests import RQTestCase
from tests.fixtures import div_by_zero, say_hello


class TestGroup(RQTestCase):
    job_1_data = Queue.prepare_data(say_hello, job_id='job1')
    job_2_data = Queue.prepare_data(say_hello, job_id='job2')
    job_a_data = Queue.prepare_data(say_hello, args=('A',), job_id='job-a')
    job_b_data = Queue.prepare_data(say_hello, args=('B',), job_id='job-b')
    job_c_data = Queue.prepare_data(say_hello, args=('C',), job_id='job-c')
    job_fail_data = Queue.prepare_data(div_by_zero, args=(0,), job_id='job-fail')
    manual_job_data = Queue.prepare_data(say_hello, args=('manual',), job_id='manual')

    def test_create_group(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        assert isinstance(group, Group)
        assert len(group.get_jobs()) == 2
        q.empty()

    def test_group_cleanup_with_no_jobs(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        assert len(group.get_jobs()) == 0
        group.cleanup()
        assert len(group.get_jobs()) == 0
        q.empty()

    def test_group_repr(self):
        group = Group.create(name='foo', connection=self.connection)
        assert group.__repr__() == 'Group(id=foo)'

    def test_group_jobs(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        self.assertCountEqual(group.get_jobs(), jobs)
        q.empty()

    def test_fetch_group(self):
        q = Queue(connection=self.connection)
        enqueued_group = Group.create(connection=self.connection)
        enqueued_group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        fetched_group = Group.fetch(enqueued_group.name, self.connection)
        self.assertCountEqual(enqueued_group.get_jobs(), fetched_group.get_jobs())
        assert len(fetched_group.get_jobs()) == 2
        q.empty()

    def test_add_jobs(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        job2 = group.enqueue_many(q, [self.job_1_data, self.job_2_data])[0]
        assert job2 in group.get_jobs()
        self.assertEqual(job2.group_id, group.name)
        q.empty()

    def test_jobs_added_to_group_key(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        job_ids = [job.id for job in group.get_jobs()]
        jobs = list({as_text(job) for job in self.connection.smembers(group.key)})
        self.assertCountEqual(jobs, job_ids)
        q.empty()

    def test_group_id_added_to_jobs(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [self.job_1_data])
        assert jobs[0].group_id == group.name
        fetched_job = Job.fetch(jobs[0].id, connection=self.connection)
        assert fetched_job.group_id == group.name

    def test_deleted_jobs_removed_from_group(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_1_data, self.job_2_data])
        job = group.get_jobs()[0]
        job.delete()
        group.cleanup()
        redis_jobs = list({as_text(job) for job in self.connection.smembers(group.key)})
        assert job.id not in redis_jobs
        assert job not in group.get_jobs()

    def test_group_added_to_registry(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_1_data])
        redis_groups = {as_text(group) for group in self.connection.smembers('rq:groups')}
        assert group.name in redis_groups
        q.empty()

    @pytest.mark.slow
    def test_expired_jobs_removed_from_group(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [short_lived_job, self.job_1_data])
        w.work(burst=True, max_jobs=1)
        sleep(2)
        w.run_maintenance_tasks()
        group.cleanup()
        assert len(group.get_jobs()) == 1
        assert self.job_1_data.job_id in [job.id for job in group.get_jobs()]
        q.empty()

    @pytest.mark.slow
    def test_empty_group_removed_from_group_list(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [short_lived_job])
        w.work(burst=True, max_jobs=1)
        sleep(2)
        w.run_maintenance_tasks()
        redis_groups = {as_text(group) for group in self.connection.smembers('rq:groups')}
        assert group.name not in redis_groups

    @pytest.mark.slow
    def test_fetch_expired_group_raises_error(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [short_lived_job])
        w.work(burst=True, max_jobs=1)
        sleep(2)
        w.run_maintenance_tasks()
        self.assertRaises(NoSuchGroupError, Group.fetch, group.name, group.connection)
        q.empty()

    def test_get_group_key(self):
        group = Group(name='foo', connection=self.connection)
        self.assertEqual(Group.get_key(group.name), 'rq:group:foo')

    def test_all_returns_all_groups(self):
        q = Queue(connection=self.connection)
        group1 = Group.create(name='group1', connection=self.connection)
        Group.create(name='group2', connection=self.connection)
        group1.enqueue_many(q, [self.job_1_data, self.job_2_data])
        all_groups = Group.all(self.connection)
        assert len(all_groups) == 1
        assert 'group1' in [group.name for group in all_groups]
        assert 'group2' not in [group.name for group in all_groups]

    def test_all_deletes_missing_groups(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        jobs = group.enqueue_many(q, [self.job_1_data])
        jobs[0].delete()
        assert not self.connection.exists(Group.get_key(group.name))
        assert Group.all(connection=self.connection) == []

    def test_get_result_none_initially(self):
        group = Group.create(connection=self.connection)
        assert group.get_result() is None

    def test_settle_returns_none_while_results_pending(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_a_data, self.job_b_data])

        result = group.settle(['job-a', 'job-b'])
        assert result is None
        # Nothing written by settle: roster and result ledger stay untouched
        assert not self.connection.exists(group.roster_key)
        assert not self.connection.exists(group.result_key)
        q.empty()

    def test_settle_submits_complete_group_result(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_a_data, self.job_b_data])
        w.work(burst=True)

        result = group.settle(['job-a', 'job-b'])
        assert isinstance(result, GroupResult)
        assert result.roster == ['job-a', 'job-b']
        assert result.results == {'job-a': 'Hi there, A!', 'job-b': 'Hi there, B!'}
        # The same ledger is readable back through get_result
        fetched = group.get_result()
        assert fetched is not None
        assert fetched.roster == result.roster
        assert fetched.results == result.results
        q.empty()

    def test_settle_converges_for_same_roster(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_a_data, self.job_b_data])
        w.work(burst=True)

        first = group.settle(['job-a', 'job-b'])
        stored_created_at = self.connection.hget(group.result_key, 'created_at')

        # Interrupted/repeated runs for the same roster return the same result
        second = group.settle(['job-a', 'job-b'])
        assert first == second
        assert first.created_at == second.created_at
        assert self.connection.hget(group.result_key, 'created_at') == stored_created_at
        q.empty()

    def test_settle_keeps_old_result_until_grown_roster_settles(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_a_data, self.job_b_data])
        w.work(burst=True)
        group.settle(['job-a', 'job-b'])

        # A new member is enqueued but hasn't run yet: no submission, old result stays
        group.enqueue_many(q, [self.job_c_data])
        assert group.settle(['job-a', 'job-b', 'job-c']) is None
        previous = group.get_result()
        assert previous is not None and previous.roster == ['job-a', 'job-b']

        # Once the new member succeeds, the ledger is replaced with the full roster
        w.work(burst=True)
        updated = group.settle(['job-a', 'job-b', 'job-c'])
        assert updated is not None
        assert updated.roster == ['job-a', 'job-b', 'job-c']
        assert updated.results == {'job-a': 'Hi there, A!', 'job-b': 'Hi there, B!', 'job-c': 'Hi there, C!'}
        q.empty()

    def test_settle_removes_dropped_member_but_keeps_job(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_a_data, self.job_b_data])
        w.work(burst=True)
        group.settle(['job-a', 'job-b'])

        result = group.settle(['job-a'])
        assert result is not None
        assert result.results == {'job-a': 'Hi there, A!'}

        group_members = {as_text(j) for j in self.connection.smembers(group.key)}
        assert 'job-a' in group_members
        assert 'job-b' not in group_members
        # The dropped member's job and its result are left in Redis
        assert Job.fetch('job-b', connection=self.connection) is not None
        assert self.connection.exists(Result.get_key('job-b'))
        q.empty()

    def test_settle_preserves_manually_added_members(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_a_data, self.job_b_data, self.manual_job_data])
        w.work(burst=True)

        # settle with the first roster: manual member, though outside the roster, stays
        group.settle(['job-a', 'job-b'])
        group_members = {as_text(j) for j in self.connection.smembers(group.key)}
        assert 'manual' in group_members

        # A later roster that drops job-b still leaves the manual member untouched
        group.settle(['job-a'])
        group_members = {as_text(j) for j in self.connection.smembers(group.key)}
        assert 'manual' in group_members
        assert 'job-b' not in group_members
        assert Job.fetch('manual', connection=self.connection) is not None
        q.empty()

    def test_settle_raises_for_unknown_member_without_writing(self):
        group = Group.create(connection=self.connection)
        with pytest.raises(InvalidRosterError):
            group.settle(['does-not-exist'])
        assert not self.connection.exists(group.roster_key)
        assert not self.connection.exists(group.result_key)
        assert group.name not in {as_text(g) for g in self.connection.smembers('rq:groups')}

    def test_settle_raises_for_empty_or_duplicate_roster(self):
        q = Queue(connection=self.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_a_data])

        with pytest.raises(InvalidRosterError):
            group.settle([])
        with pytest.raises(InvalidRosterError):
            group.settle(['job-a', 'job-a'])

        assert not self.connection.exists(group.roster_key)
        assert not self.connection.exists(group.result_key)
        q.empty()

    def test_settle_does_not_submit_when_member_failed(self):
        q = Queue(connection=self.connection)
        w = SimpleWorker([q], connection=q.connection)
        group = Group.create(connection=self.connection)
        group.enqueue_many(q, [self.job_fail_data])
        w.work(burst=True)

        assert group.settle(['job-fail']) is None
        assert not self.connection.exists(group.result_key)
        q.empty()
