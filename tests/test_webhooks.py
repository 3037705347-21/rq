import json
from datetime import datetime, timedelta, timezone
from unittest.mock import ANY, patch

from rq.defaults import (
    DEFAULT_WEBHOOK_DELIVERED_TTL,
    DEFAULT_WEBHOOK_DELIVERY_MAX_AGE,
    DEFAULT_WEBHOOK_RETRY_INITIAL_DELAY,
)
from rq.job import Job, JobStatus, Retry, requeue_job
from rq.queue import Queue
from rq.registry import StartedJobRegistry
from rq.utils import current_timestamp
from rq.webhook import Webhook
from rq.webhook_delivery import (
    DELIVERY_KEY_TEMPLATE,
    PENDING_DELIVERIES_KEY,
    RECONCILE_LOCK_KEY,
    STATE_DEAD,
    STATE_DELIVERED,
    STATE_PENDING,
    WebhookDelivery,
    reconcile_pending_webhooks,
)
from rq.worker import SimpleWorker
from tests import RQTestCase

from .fixtures import div_by_zero, fail_while_retries_remain, returns_retry, say_hello


class WebhookTestCase(RQTestCase):
    def test_init(self):
        # Defaults
        webhook = Webhook('http://example.com/hook', 'finished')
        self.assertEqual(webhook.url, 'http://example.com/hook')
        self.assertEqual(webhook.job_status, 'finished')
        self.assertEqual(webhook.method, 'GET')
        self.assertIsNone(webhook.headers)
        self.assertEqual(webhook.timeout, 10)

        # All fields set explicitly
        webhook = Webhook(
            'https://example.com/hook',
            'failed',
            method='POST',
            headers={'Authorization': 'Bearer token'},
            timeout=5,
        )
        self.assertEqual(webhook.job_status, 'failed')
        self.assertEqual(webhook.method, 'POST')
        self.assertEqual(webhook.headers, {'Authorization': 'Bearer token'})
        self.assertEqual(webhook.timeout, 5)

    def test_url_validation(self):
        with self.assertRaises(ValueError):
            Webhook('', 'finished')
        with self.assertRaises(ValueError):
            Webhook('example.com/hook', 'finished')
        with self.assertRaises(ValueError):
            Webhook('ftp://example.com', 'finished')
        with self.assertRaises(ValueError):
            Webhook('http://', 'finished')
        with self.assertRaises(ValueError):
            Webhook(None, 'finished')

    def test_job_status_validation(self):
        with self.assertRaises(ValueError):
            Webhook('http://example.com', None)
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'stopped')

    def test_method_validation(self):
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', method='PUT')
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', method='get')
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', method=None)

    def test_headers_validation(self):
        with self.assertRaises(TypeError):
            Webhook('http://example.com', 'finished', headers=[('X-Token', 'secret')])

    def test_timeout_validation(self):
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', timeout=-1)
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', timeout='10')
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', timeout=1.5)

    def test_to_dict(self):
        webhook = Webhook('http://example.com', 'failed', method='POST', timeout=5)
        self.assertEqual(
            webhook.to_dict(),
            {
                'url': 'http://example.com',
                'job_status': 'failed',
                'method': 'POST',
                'headers': None,
                'timeout': 5,
            },
        )

    def test_from_dict(self):
        # Round-trips a fully-specified webhook
        webhook = Webhook('http://example.com', 'finished', method='POST', headers={'X-A': 'b'}, timeout=3)
        self.assertEqual(Webhook.from_dict(webhook.to_dict()), webhook)

        # Fills defaults for missing optional fields
        webhook = Webhook.from_dict({'url': 'http://example.com', 'job_status': 'failed'})
        self.assertEqual(webhook, Webhook('http://example.com', 'failed'))

        # Re-validates its input
        with self.assertRaises(ValueError):
            Webhook.from_dict({'url': 'http://example.com', 'job_status': 'stopped'})

    def test_get_payload(self):
        job = Job.create(say_hello, connection=self.connection)
        job.enqueued_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        job.ended_at = datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)

        # Finished: job metadata only (the result is intentionally not included)
        payload = Webhook('http://example.com', 'finished').get_payload(job)
        self.assertEqual(
            payload,
            {
                'job_id': job.id,
                'func_name': 'tests.fixtures.say_hello',
                'status': 'finished',
                'enqueued_at': job.enqueued_at.isoformat(),
                'ended_at': job.ended_at.isoformat(),
            },
        )
        json.dumps(payload)  # the whole payload must be JSON-serializable

        # Failed: includes exc_info from the supplied exception string
        failed_webhook = Webhook('http://example.com', 'failed')
        payload = failed_webhook.get_payload(job, exc_string='Traceback: division by zero')
        self.assertEqual(payload['status'], 'failed')
        self.assertEqual(payload['exc_info'], 'Traceback: division by zero')
        json.dumps(payload)

        # Failed: exc_info is None when no exception string is supplied
        payload = failed_webhook.get_payload(job)
        self.assertIsNone(payload['exc_info'])
        json.dumps(payload)

    def test_send_get(self):
        job = Job.create(say_hello, connection=self.connection)
        webhook = Webhook('http://example.com/hook', 'finished', headers={'X-Token': 'secret'}, timeout=3)

        with patch('rq.webhook.urlopen') as urlopen_mock:
            webhook.send(job)

        request = urlopen_mock.call_args.args[0]
        self.assertEqual(request.full_url, 'http://example.com/hook')
        self.assertEqual(request.get_method(), 'GET')
        self.assertIsNone(request.data)
        self.assertEqual(request.get_header('X-token'), 'secret')
        self.assertEqual(urlopen_mock.call_args.kwargs['timeout'], 3)

    def test_send_post(self):
        job = Job.create(say_hello, connection=self.connection)
        webhook = Webhook('http://example.com/hook', 'failed', method='POST')

        with patch('rq.webhook.urlopen') as urlopen_mock:
            webhook.send(job, exc_string='boom')

        request = urlopen_mock.call_args.args[0]
        self.assertEqual(request.get_method(), 'POST')
        self.assertEqual(request.get_header('Content-type'), 'application/json')
        body = json.loads(request.data.decode('utf-8'))
        self.assertEqual(body['job_id'], job.id)
        self.assertEqual(body['exc_info'], 'boom')

    def test_send_swallows_errors(self):
        """A dead endpoint must never raise out of send()"""
        job = Job.create(say_hello, connection=self.connection)
        webhook = Webhook('http://example.com/hook', 'finished')

        with patch('rq.webhook.urlopen', side_effect=ConnectionError('endpoint down')):
            webhook.send(job)  # should not raise


class JobWebhookTestCase(RQTestCase):
    def test_create_with_webhooks(self):
        webhooks = [
            Webhook('http://example.com/done', 'finished'),
            Webhook('http://example.com/fail', 'failed'),
        ]
        job = Job.create(say_hello, connection=self.connection, webhooks=webhooks)
        self.assertEqual(job.webhooks, webhooks)

    def test_create_rejects_invalid_webhooks(self):
        # A bare Webhook (not wrapped in a list)
        with self.assertRaises(TypeError):
            Job.create(say_hello, connection=self.connection, webhooks=Webhook('http://example.com', 'finished'))

        # A list containing non-Webhook items
        with self.assertRaises(TypeError):
            Job.create(say_hello, connection=self.connection, webhooks=['http://example.com'])

    def test_webhooks_survive_redis_round_trip(self):
        webhooks = [
            Webhook('http://example.com/done', 'finished', method='POST', headers={'X-A': 'b'}, timeout=5),
            Webhook('http://example.com/fail', 'failed'),
        ]
        job = Job.create(say_hello, connection=self.connection, webhooks=webhooks)
        job.save()

        fetched_job = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(fetched_job.webhooks, webhooks)

    def test_missing_or_corrupted_webhooks_field_restores_empty_list(self):
        # Missing field (e.g. jobs saved by older RQ versions)
        job = Job.create(say_hello, connection=self.connection)
        job.save()
        self.assertEqual(Job.fetch(job.id, connection=self.connection).webhooks, [])

        # Corrupted field falls back to [] instead of raising
        self.connection.hset(job.key, 'webhooks', b'not-json')
        self.assertEqual(Job.fetch(job.id, connection=self.connection).webhooks, [])

    def test_send_webhooks(self):
        finished_webhook = Webhook('http://example.com/done', 'finished')
        failed_webhook = Webhook('http://example.com/fail', 'failed')
        job = Job.create(say_hello, connection=self.connection, webhooks=[finished_webhook, failed_webhook])

        # Only the matching webhook is sent
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            job.send_webhooks('finished')
        send_mock.assert_called_once_with(finished_webhook, job, exc_string=None, idempotency_key=ANY)

        # JobStatus enum matches, and exc_string is forwarded to the matching webhook
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            job.send_webhooks(JobStatus.FAILED, exc_string='boom')
        send_mock.assert_called_once_with(failed_webhook, job, exc_string='boom', idempotency_key=ANY)


class QueueWebhookTestCase(RQTestCase):
    """Webhooks must thread through every enqueue path onto the persisted job."""

    def setUp(self):
        super().setUp()
        self.queue = Queue(connection=self.connection)
        self.webhooks = [
            Webhook('http://example.com/done', 'finished'),
            Webhook('http://example.com/fail', 'failed'),
        ]

    def test_enqueue(self):
        job = self.queue.enqueue(say_hello, webhooks=self.webhooks)
        self.assertEqual(job.webhooks, self.webhooks)

    def test_enqueue_call(self):
        job = self.queue.enqueue_call(say_hello, webhooks=self.webhooks)
        self.assertEqual(job.webhooks, self.webhooks)

    def test_enqueue_at(self):
        job = self.queue.enqueue_at(datetime.now(timezone.utc), say_hello, webhooks=self.webhooks)
        self.assertEqual(job.webhooks, self.webhooks)

    def test_enqueue_in(self):
        job = self.queue.enqueue_in(timedelta(seconds=60), say_hello, webhooks=self.webhooks)
        self.assertEqual(job.webhooks, self.webhooks)

    def test_enqueue_many(self):
        job_data = Queue.prepare_data(say_hello, webhooks=self.webhooks)
        job = self.queue.enqueue_many([job_data])[0]
        self.assertEqual(job.webhooks, self.webhooks)

    def test_webhooks_persisted(self):
        """Webhooks survive enqueue + a fresh fetch from Redis."""
        job = self.queue.enqueue(say_hello, webhooks=self.webhooks)
        fetched_job = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(fetched_job.webhooks, self.webhooks)


class WorkerWebhookTestCase(RQTestCase):
    """The worker dispatches matching webhooks once the job reaches a terminal state.

    These tests assert which webhooks the worker hands to `Webhook.send` (and with what
    `exc_string`); the HTTP request and error-swallowing mechanics are covered by `WebhookTestCase`.
    """

    def setUp(self):
        super().setUp()
        self.queue = Queue(connection=self.connection)
        self.worker = SimpleWorker([self.queue], connection=self.connection)
        self.finished_webhook = Webhook('http://example.com/done', 'finished')
        self.failed_webhook = Webhook('http://example.com/fail', 'failed')

    def test_terminal_status_fires_only_matching_webhook(self):
        # A successful job fires only the finished webhook
        self.queue.enqueue(say_hello, webhooks=[self.finished_webhook, self.failed_webhook])
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.worker.work(burst=True)
        send_mock.assert_called_once_with(self.finished_webhook, ANY, exc_string=ANY, idempotency_key=ANY)

        # A failing job fires only the failed webhook, with the exception string forwarded
        self.queue.enqueue(div_by_zero, 1, webhooks=[self.finished_webhook, self.failed_webhook])
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.worker.work(burst=True)
        send_mock.assert_called_once_with(self.failed_webhook, ANY, exc_string=ANY, idempotency_key=ANY)
        self.assertIn('ZeroDivisionError', send_mock.call_args.kwargs['exc_string'])

    def test_retry_exhausted_fires_failed_once(self):
        """Intermediate retry attempts don't fire; only the terminal failure does."""
        self.queue.enqueue(div_by_zero, 1, retry=Retry(max=1, interval=0), webhooks=[self.failed_webhook])

        # First attempt fails and is requeued for retry: the failed webhook must not fire
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.worker.work(burst=True, max_jobs=1)
        send_mock.assert_not_called()

        # Draining the requeued job reaches terminal failure: now it fires exactly once
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.worker.work(burst=True)
        send_mock.assert_called_once_with(self.failed_webhook, ANY, exc_string=ANY, idempotency_key=ANY)

    def test_retry_then_success_skips_failed(self):
        """A job that fails then succeeds fires finished, never failed."""
        self.queue.enqueue(
            fail_while_retries_remain,
            retry=Retry(max=1, interval=0),
            webhooks=[self.finished_webhook, self.failed_webhook],
        )
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.worker.work(burst=True)
        send_mock.assert_called_once_with(self.finished_webhook, ANY, exc_string=ANY, idempotency_key=ANY)

    def test_return_based_retry_exhaustion_fires_failed(self):
        """A job returning Retry until exhausted fires the failed webhook on terminal failure."""
        self.queue.enqueue(returns_retry, webhooks=[self.finished_webhook, self.failed_webhook])
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.worker.work(burst=True)
        send_mock.assert_called_once_with(self.failed_webhook, ANY, exc_string=ANY, idempotency_key=ANY)


class SyncWebhookTestCase(RQTestCase):
    """Jobs executed synchronously (is_async=False) also fire webhooks, since they bypass the worker."""

    def setUp(self):
        super().setUp()
        self.queue = Queue(connection=self.connection, is_async=False)
        self.finished_webhook = Webhook('http://example.com/done', 'finished')
        self.failed_webhook = Webhook('http://example.com/fail', 'failed')

    def test_finished_fires_only_finished(self):
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.queue.enqueue(say_hello, webhooks=[self.finished_webhook, self.failed_webhook])
        send_mock.assert_called_once_with(self.finished_webhook, ANY, exc_string=ANY, idempotency_key=ANY)

    def test_failed_fires_only_failed_with_exc_info(self):
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.queue.enqueue(div_by_zero, 1, webhooks=[self.finished_webhook, self.failed_webhook])
        send_mock.assert_called_once_with(self.failed_webhook, ANY, exc_string=ANY, idempotency_key=ANY)
        self.assertIn('ZeroDivisionError', send_mock.call_args.kwargs['exc_string'])


class AbandonedJobWebhookTestCase(RQTestCase):
    """StartedJobRegistry.cleanup() fires the failed webhook when an abandoned job is moved
    to the FailedJobRegistry, matching the worker and sync paths."""

    def setUp(self):
        super().setUp()
        self.queue = Queue(connection=self.connection)
        self.registry = StartedJobRegistry(connection=self.connection)
        self.finished_webhook = Webhook('http://example.com/done', 'finished')
        self.failed_webhook = Webhook('http://example.com/fail', 'failed')

    def test_abandoned_terminal_failure_fires_failed_webhook(self):
        job = self.queue.enqueue(say_hello, webhooks=[self.finished_webhook, self.failed_webhook])
        job.set_status(JobStatus.STARTED)
        self.connection.zadd(self.registry.key, {f'{job.id}:execution_id': 1})

        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.registry.cleanup()
        send_mock.assert_called_once_with(self.failed_webhook, ANY, exc_string=ANY, idempotency_key=ANY)

    def test_abandoned_retry_does_not_fire_failed_webhook(self):
        """A job with retries left is requeued, not failed, so no webhook fires."""
        job = self.queue.enqueue(div_by_zero, 1, retry=Retry(max=1), webhooks=[self.failed_webhook])
        job.set_status(JobStatus.STARTED)
        self.connection.zadd(self.registry.key, {f'{job.id}:execution_id': 1})

        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.registry.cleanup()
        send_mock.assert_not_called()


def _delivery_keys(connection):
    # The pattern also matches delivery-lock keys, which must be filtered out.
    return [
        key
        for key in connection.scan_iter('rq:webhook:delivery:*')
        if not key.decode().startswith('rq:webhook:delivery-lock:')
    ]


def _load_delivery(connection, delivery_id: str) -> WebhookDelivery:
    return WebhookDelivery.from_hash(connection.hgetall(DELIVERY_KEY_TEMPLATE.format(delivery_id=delivery_id)))


class WebhookDeliveryRecordTestCase(RQTestCase):
    """Every terminal webhook gets a durable record that tracks delivery state."""

    def setUp(self):
        super().setUp()
        self.queue = Queue(connection=self.connection)
        self.worker = SimpleWorker([self.queue], connection=self.connection)

    def test_successful_send_is_marked_delivered_and_removed_from_pending(self):
        webhook = Webhook('http://example.com/done', 'finished', method='POST')
        self.queue.enqueue(say_hello, webhooks=[webhook])

        with patch.object(Webhook, 'send', autospec=True, return_value=True) as send_mock:
            self.worker.work(burst=True)

        self.assertEqual(self.connection.zcard(PENDING_DELIVERIES_KEY), 0)
        delivery_keys = _delivery_keys(self.connection)
        self.assertEqual(len(delivery_keys), 1)
        delivery = WebhookDelivery.from_hash(self.connection.hgetall(delivery_keys[0]))
        self.assertEqual(delivery.state, STATE_DELIVERED)
        self.assertEqual(delivery.attempts, 1)
        self.assertIsNotNone(delivery.delivered_at)
        # Delivered records are kept briefly for audit/dedup, then expire.
        self.assertLessEqual(self.connection.ttl(delivery.key), DEFAULT_WEBHOOK_DELIVERED_TTL)
        self.assertGreater(self.connection.ttl(delivery.key), 0)
        # The idempotency key identifies this exact job/webhook/terminal occurrence.
        idempotency_key = send_mock.call_args.kwargs['idempotency_key']
        self.assertTrue(idempotency_key.startswith(f'{send_mock.call_args.args[1].id}:finished:0:'))
        self.assertEqual(idempotency_key, delivery.id)
        # No claim lock is left behind.
        self.assertEqual(self.connection.keys('rq:webhook:delivery-lock:*'), [])

    def test_failed_send_stays_pending_with_exponential_backoff(self):
        webhook = Webhook('http://example.com/done', 'finished')
        self.queue.enqueue(say_hello, webhooks=[webhook])

        with patch.object(Webhook, 'send', autospec=True, return_value=False):
            self.worker.work(burst=True)

        pending = [member.decode() for member in self.connection.zrange(PENDING_DELIVERIES_KEY, 0, -1)]
        self.assertEqual(len(pending), 1)
        delivery = _load_delivery(self.connection, pending[0])
        self.assertEqual(delivery.state, STATE_PENDING)
        self.assertEqual(delivery.attempts, 1)
        self.assertIn('request failed', delivery.last_error)
        self.assertGreaterEqual(delivery.next_retry_at, current_timestamp() + DEFAULT_WEBHOOK_RETRY_INITIAL_DELAY - 1)
        self.assertEqual(self.connection.keys('rq:webhook:delivery-lock:*'), [])

    def test_failed_post_payload_is_snapshotted(self):
        webhook = Webhook('http://example.com/fail', 'failed', method='POST')
        self.queue.enqueue(div_by_zero, 1, webhooks=[webhook])

        with patch.object(Webhook, 'send', autospec=True, return_value=True):
            self.worker.work(burst=True)

        delivery = WebhookDelivery.from_hash(self.connection.hgetall(_delivery_keys(self.connection)[0]))
        self.assertEqual(delivery.method, 'POST')
        self.assertEqual(delivery.payload['status'], 'failed')
        self.assertIn('ZeroDivisionError', delivery.payload['exc_info'])
        self.assertEqual(delivery.payload['job_id'], delivery.job_id)

    def test_no_records_for_non_terminal_or_unrun_jobs(self):
        # An attempt that will be retried is not terminal: no record and no send.
        self.queue.enqueue(
            div_by_zero, 1, retry=Retry(max=1, interval=0), webhooks=[Webhook('http://example.com/fail', 'failed')]
        )
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.worker.work(burst=True, max_jobs=1)
        send_mock.assert_not_called()
        self.assertEqual(_delivery_keys(self.connection), [])
        self.assertEqual(self.connection.zcard(PENDING_DELIVERIES_KEY), 0)

        # A queued job that never runs also has no delivery records (queue is not
        # drained again here).
        self.queue.enqueue(say_hello, webhooks=[Webhook('http://example.com/done', 'finished')])
        self.assertEqual(_delivery_keys(self.connection), [])

    def test_same_terminal_event_registered_twice_keeps_one_record(self):
        job = Job.create(
            say_hello,
            connection=self.connection,
            webhooks=[Webhook('http://example.com/done', 'finished')],
        )
        job.ended_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
        first = job.persist_webhook_deliveries(JobStatus.FINISHED)
        second = job.persist_webhook_deliveries(JobStatus.FINISHED)
        self.assertEqual(first[0].id, second[0].id)
        self.assertEqual(self.connection.zcard(PENDING_DELIVERIES_KEY), 1)

    def test_idempotency_key_header_on_request(self):
        job = Job.create(say_hello, connection=self.connection)
        webhook = Webhook('http://example.com/hook', 'finished', headers={'X-Token': 'secret'})

        with patch('rq.webhook.urlopen') as urlopen_mock:
            webhook.send(job, idempotency_key='delivery-123')
        request = urlopen_mock.call_args.args[0]
        self.assertEqual(request.get_header('Idempotency-key'), 'delivery-123')

        # Without a key, no header is added.
        with patch('rq.webhook.urlopen') as urlopen_mock:
            webhook.send(job)
        self.assertIsNone(urlopen_mock.call_args.args[0].get_header('Idempotency-key'))


class WebhookDeliveryReconcileTestCase(RQTestCase):
    """Failed deliveries converge once the endpoint recovers, without duplicates."""

    def setUp(self):
        super().setUp()
        self.queue = Queue(connection=self.connection)
        self.worker = SimpleWorker([self.queue], connection=self.connection)
        self.webhook = Webhook('http://example.com/fail', 'failed', method='POST')

    def _terminal_failure(self, *, send_succeeds=False):
        """Enqueues+runs a failing job with the endpoint down (or up); returns its delivery id."""
        self.queue.enqueue(div_by_zero, 1, webhooks=[self.webhook])
        with patch.object(Webhook, 'send', autospec=True, return_value=send_succeeds):
            self.worker.work(burst=True)
        ids = [member.decode() for member in self.connection.zrange(PENDING_DELIVERIES_KEY, 0, -1)]
        if ids:
            return ids[0]
        # Already delivered: find the single record.
        return WebhookDelivery.from_hash(self.connection.hgetall(_delivery_keys(self.connection)[0])).id

    def _make_due(self, delivery_id: str):
        self.connection.zadd(PENDING_DELIVERIES_KEY, {delivery_id: current_timestamp() - 1})

    def test_not_due_deliveries_are_skipped(self):
        delivery_id = self._terminal_failure()
        with patch('rq.webhook_delivery.send_webhook', return_value=True) as send_mock:
            examined = reconcile_pending_webhooks(self.connection)
        self.assertEqual(examined, 0)
        send_mock.assert_not_called()
        self.assertEqual(_load_delivery(self.connection, delivery_id).state, STATE_PENDING)

    def test_due_delivery_is_resent_and_never_duplicated(self):
        delivery_id = self._terminal_failure()
        self._make_due(delivery_id)

        # Simulate the endpoint recovering: a fresh reconciliation pass (e.g. a
        # restarted worker) delivers the pending notification exactly once.
        with patch('rq.webhook_delivery.send_webhook', return_value=True) as send_mock:
            examined = reconcile_pending_webhooks(self.connection)
            self.assertEqual(examined, 1)
            # A second pass has nothing due and never re-sends.
            self.assertEqual(reconcile_pending_webhooks(self.connection), 0)
        send_mock.assert_called_once()
        self.assertEqual(send_mock.call_args.kwargs['idempotency_key'], delivery_id)

        delivery = _load_delivery(self.connection, delivery_id)
        self.assertEqual(delivery.state, STATE_DELIVERED)
        self.assertEqual(self.connection.zcard(PENDING_DELIVERIES_KEY), 0)

    def test_still_broken_endpoint_keeps_retrying_with_backoff(self):
        delivery_id = self._terminal_failure()
        self._make_due(delivery_id)

        with patch('rq.webhook_delivery.send_webhook', return_value=False):
            reconcile_pending_webhooks(self.connection)

        delivery = _load_delivery(self.connection, delivery_id)
        self.assertEqual(delivery.state, STATE_PENDING)
        self.assertEqual(delivery.attempts, 2)
        score = self.connection.zscore(PENDING_DELIVERIES_KEY, delivery_id)
        self.assertGreaterEqual(score, current_timestamp() + 2 * DEFAULT_WEBHOOK_RETRY_INITIAL_DELAY - 1)

    def test_global_reconcile_lock_skips_the_pass(self):
        self.connection.set(RECONCILE_LOCK_KEY, 'other', nx=True, ex=60)
        self.assertEqual(reconcile_pending_webhooks(self.connection), 0)

    def test_batch_size_caps_deliveries_per_pass(self):
        ids = [self._terminal_failure() for _ in range(3)]
        for delivery_id in ids:
            self._make_due(delivery_id)

        with patch('rq.webhook_delivery.send_webhook', return_value=True):
            examined = reconcile_pending_webhooks(self.connection, batch_size=2)
        self.assertEqual(examined, 2)
        self.assertEqual(self.connection.zcard(PENDING_DELIVERIES_KEY), 1)

    def test_orphan_pending_member_is_cleaned_up(self):
        delivery_id = self._terminal_failure()
        self._make_due(delivery_id)
        self.connection.delete(DELIVERY_KEY_TEMPLATE.format(delivery_id=delivery_id))
        self.assertEqual(reconcile_pending_webhooks(self.connection), 1)
        self.assertEqual(self.connection.zcard(PENDING_DELIVERIES_KEY), 0)

    def test_non_pending_member_is_removed_without_resending(self):
        delivery_id = self._terminal_failure(send_succeeds=True)
        # A stale re-added index member for an already delivered record.
        self.connection.zadd(PENDING_DELIVERIES_KEY, {delivery_id: current_timestamp() - 1})

        with patch('rq.webhook_delivery.send_webhook', return_value=True) as send_mock:
            examined = reconcile_pending_webhooks(self.connection)
        self.assertEqual(examined, 1)
        send_mock.assert_not_called()
        self.assertEqual(self.connection.zcard(PENDING_DELIVERIES_KEY), 0)

    def test_delivery_older_than_max_age_is_marked_dead(self):
        delivery_id = self._terminal_failure()
        delivery = _load_delivery(self.connection, delivery_id)
        delivery.created_at = datetime.now(timezone.utc) - timedelta(seconds=DEFAULT_WEBHOOK_DELIVERY_MAX_AGE + 1)
        self.connection.hset(delivery.key, mapping=delivery.to_hash())
        self._make_due(delivery_id)

        with patch('rq.webhook_delivery.send_webhook', return_value=False):
            reconcile_pending_webhooks(self.connection)

        self.assertEqual(_load_delivery(self.connection, delivery_id).state, STATE_DEAD)
        self.assertEqual(self.connection.zcard(PENDING_DELIVERIES_KEY), 0)
        self.assertLessEqual(self.connection.ttl(delivery.key), DEFAULT_WEBHOOK_DELIVERED_TTL)

    def test_reconcile_sends_from_snapshot_after_job_hash_expired(self):
        delivery_id = self._terminal_failure()
        # The finished/failed job itself has been deleted (e.g. result_ttl=0),
        # but the snapshotted payload is enough to resend the notification.
        Job.fetch(delivery_id.split(':failed:')[0], connection=self.connection).delete()
        self._make_due(delivery_id)

        with patch('rq.webhook_delivery.send_webhook', return_value=True) as send_mock:
            reconcile_pending_webhooks(self.connection)

        self.assertEqual(send_mock.call_args.kwargs['payload']['job_id'], delivery_id.split(':failed:')[0])
        self.assertEqual(_load_delivery(self.connection, delivery_id).state, STATE_DELIVERED)

    def test_claim_lock_prevents_concurrent_double_send(self):
        delivery_id = self._terminal_failure()
        self._make_due(delivery_id)
        delivery = _load_delivery(self.connection, delivery_id)
        self.connection.set(delivery.lock_key, delivery_id, nx=True, ex=60)

        with patch('rq.webhook_delivery.send_webhook', return_value=True) as send_mock:
            reconcile_pending_webhooks(self.connection)
        send_mock.assert_not_called()
        self.assertEqual(_load_delivery(self.connection, delivery_id).state, STATE_PENDING)

    def test_requeue_and_fail_again_creates_distinct_deliveries(self):
        job = self.queue.enqueue(div_by_zero, 1, retry=Retry(max=1, interval=0), webhooks=[self.webhook])

        # Retried first attempt, then terminal failure within the same burst: one delivery.
        with patch.object(Webhook, 'send', autospec=True, return_value=True) as send_mock:
            self.worker.work(burst=True)
        send_mock.assert_called_once()
        self.assertEqual(len(_delivery_keys(self.connection)), 1)

        # Requeued and failed again: a second, distinct terminal notification.
        requeue_job(job.id, connection=self.connection)
        with patch.object(Webhook, 'send', autospec=True, return_value=True):
            self.worker.work(burst=True)

        records = [WebhookDelivery.from_hash(self.connection.hgetall(key)) for key in _delivery_keys(self.connection)]
        self.assertEqual(len(records), 2)
        self.assertEqual({record.state for record in records}, {STATE_DELIVERED})
        self.assertEqual(len({record.id for record in records}), 2)


class WorkerWebhookReconcileTickTestCase(RQTestCase):
    """The worker runs reconciliation on its own interval gate, without breaking job processing."""

    def setUp(self):
        super().setUp()
        self.queue = Queue(connection=self.connection)
        self.worker = SimpleWorker([self.queue], connection=self.connection)

    def test_gate_runs_once_on_startup_then_on_interval(self):
        self.assertTrue(self.worker.should_reconcile_webhook_deliveries)

        with patch('rq.worker.base.reconcile_pending_webhooks') as reconcile_mock:
            self.worker.reconcile_webhook_deliveries()
            reconcile_mock.assert_called_once()

            # Within the interval, nothing happens.
            self.assertFalse(self.worker.should_reconcile_webhook_deliveries)
            self.worker.reconcile_webhook_deliveries()
            reconcile_mock.assert_called_once()

            # After the interval elapses, another pass runs.
            self.worker.last_webhook_reconcile_at -= timedelta(seconds=self.worker.webhook_reconcile_interval + 1)
            self.assertTrue(self.worker.should_reconcile_webhook_deliveries)
            self.worker.reconcile_webhook_deliveries()
            self.assertEqual(reconcile_mock.call_count, 2)

    def test_reconcile_errors_never_escape(self):
        with patch('rq.worker.base.reconcile_pending_webhooks', side_effect=RuntimeError('redis down')):
            self.worker.reconcile_webhook_deliveries()  # must not raise
        self.assertIsNotNone(self.worker.last_webhook_reconcile_at)

    def test_starting_worker_reconciles_pending_before_processing(self):
        """A freshly started worker converges deliveries left behind by a crashed one."""
        first_worker = SimpleWorker([self.queue], connection=self.connection)
        self.queue.enqueue(div_by_zero, 1, webhooks=[Webhook('http://example.com/fail', 'failed', method='POST')])

        # First process: endpoint is down, one failed immediate attempt, durable pending record.
        with patch.object(Webhook, 'send', autospec=True, return_value=False) as first_send:
            first_worker.work(burst=True)
        first_send.assert_called_once()

        pending = [member.decode() for member in self.connection.zrange(PENDING_DELIVERIES_KEY, 0, -1)]
        self.assertEqual(len(pending), 1)
        self.connection.zadd(PENDING_DELIVERIES_KEY, {pending[0]: current_timestamp() - 1})

        # A brand-new worker (simulating restart after the endpoint recovered)
        # reconciles on startup and delivers it.
        second_worker = SimpleWorker([self.queue], connection=self.connection)
        with patch('rq.webhook_delivery.send_webhook', return_value=True) as reconcile_send:
            second_worker.work(burst=True)
        reconcile_send.assert_called_once()
        self.assertEqual(_load_delivery(self.connection, pending[0]).state, STATE_DELIVERED)
