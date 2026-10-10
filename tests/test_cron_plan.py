import json
from datetime import timedelta
from unittest.mock import patch

from rq import Queue
from rq.cron import CronScheduler
from rq.cron_plan import (
    DEFAULT_PLAN_ID,
    CronDefinition,
    CronPlan,
    ReconcileResult,
    reconcile,
)
from rq.cron_plan_registry import get_definitions, get_plan_key
from rq.exceptions import InvalidCronPlanError
from rq.webhook import Webhook
from tests import RQTestCase
from tests.fixtures import div_by_zero, do_nothing, say_hello


def interval_entry(func=say_hello, name='hello', queue_name='default', interval=60, **overrides):
    entry = {'func': func, 'queue_name': queue_name, 'interval': interval, 'name': name}
    entry.update(overrides)
    return entry


def cron_entry(func=do_nothing, name='idle', queue_name='default', cron='0 9 * * *', **overrides):
    entry = {'func': func, 'queue_name': queue_name, 'cron': cron, 'name': name}
    entry.update(overrides)
    return entry


class TestCronPlanValidation(RQTestCase):
    """Whole-batch validation rules for cron plans (no Redis writes needed)."""

    def test_valid_plan_builds_definitions(self):
        plan = CronPlan(
            [
                interval_entry(name='a', interval=60),
                cron_entry(name='b', cron='0 9 * * *', args=(1, 'x'), kwargs={'k': 2}),
            ]
        )
        self.assertEqual([d.name for d in plan.definitions], ['a', 'b'])
        self.assertEqual(len(plan), 2)

    def test_default_name_is_qualified_function_name(self):
        plan = CronPlan([interval_entry(say_hello, name='')])
        self.assertEqual(plan.definitions[0].name, 'tests.fixtures.say_hello')
        self.assertEqual(plan.definitions[0].func_name, 'tests.fixtures.say_hello')

    def test_canonical_json_is_stable(self):
        definition = CronPlan([interval_entry(interval=60)]).definitions[0]
        # Same content built twice yields the exact same string
        again = CronPlan([interval_entry(interval=60)]).definitions[0]
        self.assertEqual(definition.canonical_json(), again.canonical_json())
        # kwargs ordering does not affect the canonical representation
        unordered = CronPlan([cron_entry(kwargs={'z': 1, 'a': 2})]).definitions[0]
        ordered = CronPlan([cron_entry(kwargs={'a': 2, 'z': 1})]).definitions[0]
        self.assertEqual(unordered.canonical_json(), ordered.canonical_json())
        # The stored payload round-trips to the same definition
        decoded = CronDefinition.from_dict(json.loads(definition.canonical_json()))
        self.assertEqual(decoded.canonical_json(), definition.canonical_json())

    def test_duplicate_names_are_rejected(self):
        with self.assertRaises(InvalidCronPlanError) as ctx:
            CronPlan([interval_entry(name='dup'), cron_entry(name='dup')])
        # Both clashing entries are reported so reviewers can see them
        dup_errors = [error for error in ctx.exception.errors if 'duplicate' in error]
        self.assertEqual(len(dup_errors), 2)

    def test_duplicate_default_names_detected(self):
        # Two entries pointing at the same function without explicit names clash
        with self.assertRaises(InvalidCronPlanError) as ctx:
            CronPlan([interval_entry(say_hello, name=''), cron_entry(say_hello, name='', cron='0 9 * * *')])
        self.assertTrue(any('duplicate' in error for error in ctx.exception.errors))

    def test_duplicate_reported_even_with_other_errors(self):
        with self.assertRaises(InvalidCronPlanError) as ctx:
            CronPlan(
                [
                    interval_entry(name='dup'),
                    cron_entry(name='dup', cron='99 9 * * *'),
                ]
            )
        self.assertTrue(any('duplicate' in error for error in ctx.exception.errors))
        self.assertTrue(any('invalid cron' in error for error in ctx.exception.errors))

    def test_invalid_cron_expression_rejected(self):
        for bad_cron in ('99 9 * * *', 'not a cron', '', '*/x * * * *'):
            with self.subTest(bad_cron=bad_cron):
                with self.assertRaises(InvalidCronPlanError) as ctx:
                    CronPlan([cron_entry(cron=bad_cron)])
                self.assertTrue(any('cron' in error for error in ctx.exception.errors))

    def test_invalid_interval_rejected(self):
        for bad_interval in (0, -10, '60', 1.5, True):
            with self.subTest(bad_interval=bad_interval):
                with self.assertRaises(InvalidCronPlanError) as ctx:
                    CronPlan([interval_entry(interval=bad_interval)])
                self.assertTrue(any('interval' in error for error in ctx.exception.errors))

    def test_schedule_must_be_exclusive_and_present(self):
        with self.assertRaises(InvalidCronPlanError) as ctx:
            CronPlan([interval_entry(cron='0 9 * * *')])
        self.assertTrue(any('both interval and cron' in error for error in ctx.exception.errors))

        with self.assertRaises(InvalidCronPlanError) as ctx:
            entry = interval_entry()
            del entry['interval']
            CronPlan([entry])
        self.assertTrue(any('either interval or cron' in error for error in ctx.exception.errors))

    def test_missing_queue_and_name_rejected(self):
        with self.assertRaises(InvalidCronPlanError) as ctx:
            CronPlan([{'func': say_hello, 'queue_name': '', 'interval': 60, 'name': 'x'}])
        self.assertTrue(any('queue_name' in error for error in ctx.exception.errors))

    def test_non_serializable_payloads_rejected(self):
        with self.assertRaises(InvalidCronPlanError) as ctx:
            CronPlan([interval_entry(args=(object(),))])
        self.assertTrue(any('args' in error for error in ctx.exception.errors))

        with self.assertRaises(InvalidCronPlanError) as ctx:
            CronPlan([interval_entry(kwargs={'obj': object()})])
        self.assertTrue(any('kwargs' in error for error in ctx.exception.errors))

        with self.assertRaises(InvalidCronPlanError) as ctx:
            CronPlan([interval_entry(meta={'obj': object()})])
        self.assertTrue(any('meta' in error for error in ctx.exception.errors))

    def test_bad_payload_types_rejected(self):
        with self.assertRaises(InvalidCronPlanError) as ctx:
            CronPlan([interval_entry(args='not-a-sequence')])
        self.assertTrue(any('args' in error for error in ctx.exception.errors))

        with self.assertRaises(InvalidCronPlanError) as ctx:
            CronPlan([interval_entry(kwargs=['not', 'a', 'dict'])])
        self.assertTrue(any('kwargs' in error for error in ctx.exception.errors))

    def test_func_name_must_be_importable(self):
        with self.assertRaises(InvalidCronPlanError) as ctx:
            CronPlan([{'func_name': 'tests.fixtures.does_not_exist', 'queue_name': 'default', 'interval': 10}])
        self.assertTrue(any('cannot import' in error for error in ctx.exception.errors))

    def test_func_must_be_callable(self):
        with self.assertRaises(InvalidCronPlanError) as ctx:
            CronPlan([interval_entry(func='say_hello')])
        self.assertTrue(any('callable' in error for error in ctx.exception.errors))

    def test_webhooks_must_be_webhook_instances(self):
        with self.assertRaises(InvalidCronPlanError) as ctx:
            CronPlan([interval_entry(webhooks=['https://example.com'])])
        self.assertTrue(any('webhooks' in error for error in ctx.exception.errors))

    def test_valid_webhooks_accepted(self):
        webhook = Webhook('https://example.com/hook', 'finished')
        plan = CronPlan([interval_entry(webhooks=[webhook])])
        self.assertEqual(plan.definitions[0].webhooks, [webhook])

    def test_errors_from_multiple_entries_are_aggregated(self):
        with self.assertRaises(InvalidCronPlanError) as ctx:
            CronPlan(
                [
                    interval_entry(name='a', interval=-1),
                    cron_entry(name='b', cron='nonsense'),
                    interval_entry(name='c', queue_name=''),
                ]
            )
        self.assertGreaterEqual(len(ctx.exception.errors), 3)
        joined = str(ctx.exception)
        self.assertIn('a', joined)
        self.assertIn('b', joined)
        self.assertIn('c', joined)

    def test_empty_plan_is_valid(self):
        plan = CronPlan([])
        self.assertEqual(plan.definitions, [])

    def test_load_from_file(self):
        plan = CronPlan.load('tests/cron_plan_config.py')
        self.assertEqual(
            [d.name for d in plan.definitions],
            ['plan-hello', 'tests.fixtures.do_nothing', 'plan-failing'],
        )
        failing = plan.get_definition('plan-failing')
        self.assertIsNotNone(failing)
        self.assertEqual(failing.args, (10,))
        self.assertEqual(failing.interval, 300)

    def test_load_from_file_clears_global_registry(self):
        CronPlan.load('tests/cron_plan_config.py')
        plan_again = CronPlan.load('tests/cron_plan_config.py')
        self.assertEqual(len(plan_again), 3)


class TestCronPlanReconcile(RQTestCase):
    """Declarative reconciliation against the Redis-backed managed definitions."""

    def plan_key(self, plan_id=DEFAULT_PLAN_ID):
        return get_plan_key(plan_id)

    def test_apply_adds_definitions(self):
        plan = CronPlan([interval_entry(name='a'), cron_entry(name='b')])
        result = reconcile(plan, self.connection)
        self.assertIsInstance(result, ReconcileResult)
        self.assertEqual(result.added, ['a', 'b'])
        self.assertEqual(result.changed, [])
        self.assertEqual(result.removed, [])
        self.assertEqual(result.unchanged, [])

        stored = get_definitions(self.connection, DEFAULT_PLAN_ID)
        self.assertEqual(set(stored), {'a', 'b'})
        self.assertEqual(stored['a'], plan.definitions[0].canonical_json())

    def test_reapply_is_idempotent_and_writes_nothing(self):
        plan = CronPlan([interval_entry(name='a'), cron_entry(name='b')])
        first = reconcile(plan, self.connection)
        self.assertFalse(first.is_noop)

        # Second apply: everything unchanged and no pipeline/write is issued at all
        with patch.object(self.connection, 'pipeline', side_effect=AssertionError('no writes expected on no-op apply')):
            second = reconcile(plan, self.connection)
        self.assertTrue(second.is_noop)
        self.assertEqual(second.unchanged, ['a', 'b'])
        self.assertEqual(second.added, second.changed, second.removed)

    def test_change_schedule_is_detected(self):
        reconcile(CronPlan([interval_entry(name='a', interval=60)]), self.connection)
        result = reconcile(CronPlan([interval_entry(name='a', interval=120)]), self.connection)
        self.assertEqual(result.changed, ['a'])
        self.assertEqual(result.unchanged, [])
        stored = get_definitions(self.connection, DEFAULT_PLAN_ID)
        self.assertIn('"interval":120', stored['a'])

    def test_change_arguments_is_detected(self):
        reconcile(CronPlan([cron_entry(name='a', args=('x',))]), self.connection)
        result = reconcile(CronPlan([cron_entry(name='a', args=('y',))]), self.connection)
        self.assertEqual(result.changed, ['a'])

    def test_change_queue_is_detected(self):
        reconcile(CronPlan([interval_entry(name='a')]), self.connection)
        result = reconcile(CronPlan([interval_entry(name='a', queue_name='critical')]), self.connection)
        self.assertEqual(result.changed, ['a'])

    def test_remove_deletes_definition(self):
        reconcile(CronPlan([interval_entry(name='a'), cron_entry(name='b')]), self.connection)
        result = reconcile(CronPlan([interval_entry(name='a')]), self.connection)
        self.assertEqual(result.removed, ['b'])
        stored = get_definitions(self.connection, DEFAULT_PLAN_ID)
        self.assertEqual(set(stored), {'a'})

    def test_rename_is_remove_and_add(self):
        reconcile(CronPlan([interval_entry(name='old')]), self.connection)
        result = reconcile(CronPlan([interval_entry(name='new')]), self.connection)
        self.assertEqual(result.added, ['new'])
        self.assertEqual(result.removed, ['old'])

    def test_empty_plan_removes_all_managed_definitions(self):
        reconcile(CronPlan([interval_entry(name='a'), cron_entry(name='b')]), self.connection)
        result = reconcile(CronPlan([]), self.connection)
        self.assertEqual(result.removed, ['a', 'b'])
        self.assertEqual(self.connection.exists(self.plan_key()), 0)

    def test_invalid_plan_rejected_without_touching_live_state(self):
        good_plan = CronPlan([interval_entry(name='a', interval=60), cron_entry(name='b')])
        reconcile(good_plan, self.connection)
        before = self.connection.hgetall(self.plan_key())

        with self.assertRaises(InvalidCronPlanError):
            # Duplicate names AND an invalid cron expression
            CronPlan(
                [
                    interval_entry(name='a', interval=120),
                    interval_entry(name='a', interval=30),
                    cron_entry(name='c', cron='not valid'),
                ]
            )

        # The rejected plan was never applied: live state is byte-identical
        after = self.connection.hgetall(self.plan_key())
        self.assertEqual(before, after)

    def test_invalid_plan_issues_no_writes_even_when_state_exists(self):
        reconcile(CronPlan([interval_entry(name='a')]), self.connection)
        with patch.object(self.connection, 'pipeline', side_effect=AssertionError('invalid plan must not write')):
            with self.assertRaises(InvalidCronPlanError):
                CronPlan([interval_entry(name='a', interval=-1), interval_entry(name='a', interval=-2)])

    def test_plans_are_isolated_by_plan_id(self):
        reconcile(CronPlan([interval_entry(name='a')]), self.connection, plan_id='team-a')
        reconcile(CronPlan([cron_entry(name='b')]), self.connection, plan_id='team-b')

        # Removing 'a' from team-a's plan leaves team-b completely untouched
        result = reconcile(CronPlan([]), self.connection, plan_id='team-a')
        self.assertEqual(result.removed, ['a'])
        self.assertEqual(self.connection.exists(get_plan_key('team-a')), 0)
        team_b = get_definitions(self.connection, 'team-b')
        self.assertEqual(set(team_b), {'b'})

    def test_external_state_is_preserved(self):
        # Simulate cron state created by other means: a live scheduler hash and
        # the cron job name registry, plus a plan owned by another team.
        self.connection.hset('rq:cron_scheduler:someone-else', mapping={'hostname': 'host-1', 'pid': '4242'})
        self.connection.zadd('rq:cron_jobs', {'ad-hoc-job': 1.0})
        reconcile(CronPlan([cron_entry(name='foreign-managed')]), self.connection, plan_id='other')

        reconcile(
            CronPlan([interval_entry(name='a'), cron_entry(name='b')]),
            self.connection,
        )
        reconcile(
            CronPlan([interval_entry(name='a')]),
            self.connection,
        )  # remove 'b'

        self.assertEqual(
            self.connection.hgetall('rq:cron_scheduler:someone-else'),
            {b'hostname': b'host-1', b'pid': b'4242'},
        )
        self.assertEqual(self.connection.zscore('rq:cron_jobs', 'ad-hoc-job'), 1.0)
        self.assertEqual(set(get_definitions(self.connection, 'other')), {'foreign-managed'})

    def test_removed_job_history_is_left_for_ttl_aging(self):
        # Removing a definition stops future scheduling but does not destroy history
        self.connection.zadd('rq:cron_job:a:jobs', {'job-1': 1.0})
        reconcile(CronPlan([interval_entry(name='a')]), self.connection)
        reconcile(CronPlan([]), self.connection)
        self.assertEqual(self.connection.zscore('rq:cron_job:a:jobs', 'job-1'), 1.0)

    def test_invalid_plan_id_rejected(self):
        plan = CronPlan([interval_entry(name='a')])
        for bad_id in ('', 'bad/id', 'with space', 'a*b'):
            with self.assertRaises(ValueError):
                reconcile(plan, self.connection, plan_id=bad_id)

    def test_result_summary(self):
        plan = CronPlan([interval_entry(name='a'), cron_entry(name='b')])
        result = reconcile(plan, self.connection)
        text = result.summary()
        self.assertIn("'default'", text)
        self.assertIn('2 added', text)
        self.assertIn('+ a', text)


class TestCronDefinitionToJob(RQTestCase):
    def test_to_cron_job_resolves_function(self):
        definition = CronPlan([interval_entry(name='a')]).definitions[0]
        job = definition.to_cron_job()
        self.assertIs(job.func, say_hello)
        self.assertEqual(job.name, 'a')

    def test_to_cron_job_without_resolution_is_monitoring_only(self):
        definition = CronPlan([interval_entry(name='a')]).definitions[0]
        job = definition.to_cron_job(resolve_func=False)
        self.assertIsNone(job.func)
        self.assertEqual(job.func_name, 'tests.fixtures.say_hello')
        with self.assertRaises(ValueError):
            job.enqueue(self.connection)

    def test_unsupported_serialized_version_rejected(self):
        with self.assertRaises(ValueError):
            CronDefinition.from_dict(
                {
                    'version': 999,
                    'name': 'a',
                    'func_name': 'tests.fixtures.say_hello',
                    'queue_name': 'default',
                    'interval': 60,
                }
            )


class TestManagedScheduler(RQTestCase):
    """Plan-mode scheduler: managed definitions take effect, removals stop, ad-hoc jobs survive."""

    PLAN_ID = 'test-plan'

    def make_scheduler(self) -> CronScheduler:
        return CronScheduler(connection=self.connection, plan_id=self.PLAN_ID)

    def apply(self, entries, plan_id=PLAN_ID):
        return reconcile(CronPlan(entries), self.connection, plan_id=plan_id)

    def managed_by_name(self, scheduler: CronScheduler) -> dict:
        return {job.name: job for job in scheduler._managed_cron_jobs}

    def test_refresh_loads_managed_jobs_with_resolved_functions(self):
        self.apply(
            [
                interval_entry(say_hello, name='greet', interval=60),
                cron_entry(do_nothing, name='idle'),
            ]
        )
        scheduler = self.make_scheduler()
        self.assertTrue(scheduler.refresh_managed_jobs())

        jobs = self.managed_by_name(scheduler)
        self.assertEqual(set(jobs), {'greet', 'idle'})
        self.assertIs(jobs['greet'].func, say_hello)
        self.assertEqual(scheduler._cron_jobs, [])

    def test_managed_jobs_are_enqueued(self):
        self.apply([interval_entry(say_hello, name='greet', interval=60)])
        scheduler = self.make_scheduler()
        scheduler.refresh_managed_jobs()

        enqueued = scheduler.enqueue_jobs()
        self.assertEqual({job.name for job in enqueued}, {'greet'})
        self.assertEqual(len(Queue('default', connection=self.connection).get_jobs()), 1)

    def test_refresh_adds_changes_and_removes(self):
        self.apply(
            [
                interval_entry(say_hello, name='greet', interval=60),
                interval_entry(do_nothing, name='idle', interval=120),
            ]
        )
        scheduler = self.make_scheduler()
        scheduler.refresh_managed_jobs()
        self.assertEqual(set(self.managed_by_name(scheduler)), {'greet', 'idle'})

        # greet changes schedule, idle removed, a new cron job is added
        self.apply(
            [
                interval_entry(say_hello, name='greet', interval=300),
                cron_entry(do_nothing, name='weekly', cron='0 9 * * 1'),
            ]
        )
        scheduler.refresh_managed_jobs()
        jobs = self.managed_by_name(scheduler)
        self.assertEqual(set(jobs), {'greet', 'weekly'})
        self.assertEqual(jobs['greet'].interval, 300)

    def test_removed_job_stops_being_enqueued(self):
        self.apply([interval_entry(say_hello, name='greet', interval=60)])
        scheduler = self.make_scheduler()
        scheduler.refresh_managed_jobs()
        scheduler.enqueue_jobs()  # first run: interval jobs run immediately

        queue = Queue('default', connection=self.connection)
        queue.empty()

        self.apply([])
        self.assertTrue(scheduler.refresh_managed_jobs())
        self.assertEqual(scheduler._managed_cron_jobs, [])
        self.assertEqual(scheduler.enqueue_jobs(), [])
        self.assertEqual(len(queue.get_jobs()), 0)

    def test_ad_hoc_registered_jobs_survive_managed_refresh(self):
        self.apply([interval_entry(say_hello, name='greet', interval=60)])
        scheduler = self.make_scheduler()
        # Another team registers a temporary job directly on this scheduler
        scheduler.register(div_by_zero, 'default', args=(1,), interval=90, name='ad-hoc')

        scheduler.refresh_managed_jobs()
        scheduler.refresh_managed_jobs()
        self.assertEqual({job.name for job in scheduler.get_jobs()}, {'greet', 'ad-hoc'})
        self.assertEqual({job.name for job in scheduler.enqueue_jobs()}, {'greet', 'ad-hoc'})

        # Removing the managed job from the plan never touches the ad-hoc job
        self.apply([])
        scheduler.refresh_managed_jobs()
        self.assertEqual({job.name for job in scheduler.get_jobs()}, {'ad-hoc'})
        self.assertEqual(scheduler._cron_jobs[0].name, 'ad-hoc')

    def test_cadence_is_carried_across_refresh(self):
        self.apply([interval_entry(say_hello, name='greet', interval=60)])
        scheduler = self.make_scheduler()
        scheduler.refresh_managed_jobs()
        scheduler.enqueue_jobs()
        previous_latest = self.managed_by_name(scheduler)['greet'].latest_enqueue_time

        scheduler.refresh_managed_jobs()
        greet = self.managed_by_name(scheduler)['greet']
        self.assertEqual(greet.latest_enqueue_time, previous_latest)
        self.assertFalse(greet.should_run())
        self.assertEqual(greet.next_enqueue_time, previous_latest + timedelta(seconds=60))

    def test_refresh_failure_keeps_previous_job_set(self):
        self.apply([interval_entry(say_hello, name='greet', interval=60)])
        scheduler = self.make_scheduler()
        scheduler.refresh_managed_jobs()

        # Corrupt the stored definition: the refresh fails but the running set survives
        self.connection.hset(get_plan_key(self.PLAN_ID), mapping={'greet': 'not-valid-json'})
        self.assertFalse(scheduler.refresh_managed_jobs())
        jobs = self.managed_by_name(scheduler)
        self.assertEqual(set(jobs), {'greet'})
        self.assertIs(jobs['greet'].func, say_hello)

        # Once the good definition is restored, refreshing succeeds again
        self.apply([interval_entry(say_hello, name='greet', interval=60)])
        self.assertTrue(scheduler.refresh_managed_jobs())

    def test_refresh_import_error_keeps_previous_job_set(self):
        self.apply([interval_entry(say_hello, name='greet', interval=60)])
        scheduler = self.make_scheduler()
        scheduler.refresh_managed_jobs()

        raw = json.dumps(
            {
                'version': 1,
                'name': 'greet',
                'func_name': 'tests.fixtures.does_not_exist',
                'queue_name': 'default',
                'interval': 60,
            }
        )
        self.connection.hset(get_plan_key(self.PLAN_ID), mapping={'greet': raw})
        self.assertFalse(scheduler.refresh_managed_jobs())
        self.assertEqual(set(self.managed_by_name(scheduler)), {'greet'})

    def test_plan_mode_disabled_by_default(self):
        scheduler = CronScheduler(connection=self.connection)
        self.assertIsNone(scheduler.plan_id)
        self.assertFalse(scheduler.refresh_managed_jobs())
        with self.assertRaises(ValueError):
            scheduler.load_managed_jobs()

    def test_managed_jobs_visible_in_monitoring_state(self):
        self.apply([interval_entry(say_hello, name='greet', interval=60)])
        scheduler = self.make_scheduler()
        scheduler.refresh_managed_jobs()
        scheduler.save()

        fetched = CronScheduler.fetch(scheduler.name, connection=self.connection)
        self.assertEqual([job.name for job in fetched._managed_cron_jobs], ['greet'])
