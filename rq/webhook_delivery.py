"""Durable delivery records for terminal-state webhooks.

Every webhook due when a job reaches a terminal state is first persisted as a
pending `WebhookDelivery` (in the same Redis transaction as the terminal job
state) and only then sent over HTTP:

* successful sends flip the record to ``delivered`` and are never sent again;
* failed sends stay in the pending sorted set with an exponential backoff and
  are retried by `reconcile_pending_webhooks()` (run periodically by workers
  and available as a public API / CLI command);
* records that stay undeliverable past the maximum age are marked ``dead``.

The module only touches the ``rq:webhook:*`` key namespace, so retries never
read or mutate jobs that haven't reached a terminal state. The POST payload is
snapshotted when the delivery is created, allowing retries after the job hash
itself has expired.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

from redis import Redis
from redis.client import Pipeline

from .defaults import (
    DEFAULT_WEBHOOK_DELIVERED_TTL,
    DEFAULT_WEBHOOK_DELIVERY_MAX_AGE,
    DEFAULT_WEBHOOK_RECONCILE_BATCH_SIZE,
    DEFAULT_WEBHOOK_RECONCILE_LOCK_TTL,
    DEFAULT_WEBHOOK_RETRY_INITIAL_DELAY,
    DEFAULT_WEBHOOK_RETRY_MAX_DELAY,
)
from .utils import as_text, current_timestamp, now
from .webhook import Webhook, send_webhook

if TYPE_CHECKING:
    from .job import Job, JobStatus

logger = logging.getLogger('rq.webhook_delivery')

PENDING_DELIVERIES_KEY = 'rq:webhook:pending'
DELIVERY_KEY_TEMPLATE = 'rq:webhook:delivery:{delivery_id}'
DELIVERY_LOCK_KEY_TEMPLATE = 'rq:webhook:delivery-lock:{delivery_id}'
RECONCILE_LOCK_KEY = 'rq:webhook:reconcile-lock'

STATE_PENDING = 'pending'
STATE_DELIVERED = 'delivered'
STATE_DEAD = 'dead'

# Lua: atomically create a delivery hash and its pending index entry.
# An existing delivery (delivered or still pending) is never overwritten,
# which makes re-registering the same terminal webhook idempotent.
# KEYS[1] = delivery hash key
# KEYS[2] = pending deliveries sorted set
# ARGV[1] = delivery id (sorted set member)
# ARGV[2] = initial next-retry score (unix timestamp)
# ARGV[3] = delivery hash TTL in seconds
# ARGV[4..] = HSET field/value pairs
CREATE_DELIVERY_SCRIPT = """
if redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end
redis.call('HSET', KEYS[1], unpack(ARGV, 4))
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
return 1
"""

_MAX_ERROR_LENGTH = 1000


def _backoff_delay(attempts: int) -> int:
    """Exponential backoff after `attempts` failed sends, capped at the max."""
    delay = DEFAULT_WEBHOOK_RETRY_INITIAL_DELAY * 2 ** (attempts - 1)
    return min(delay, DEFAULT_WEBHOOK_RETRY_MAX_DELAY)


def _claim_lock_ttl(timeout: int) -> int:
    # The lock must outlive an in-flight request (plus headroom) so a crashed
    # sender can't have its delivery picked up mid-request by another process.
    return min(timeout + 30, DEFAULT_WEBHOOK_RETRY_MAX_DELAY + 120)


@dataclass
class WebhookDelivery:
    """One durable, idempotently-keyed webhook delivery attempt."""

    id: str
    job_id: str
    job_status: str
    url: str
    method: str
    timeout: int
    created_at: datetime = field(default_factory=now)
    headers: dict[str, str] | None = None
    payload: dict[str, Any] | None = None
    state: str = STATE_PENDING
    attempts: int = 0
    next_retry_at: float = field(default_factory=current_timestamp)
    last_error: str | None = None
    delivered_at: datetime | None = None
    # Transient (never persisted): the Webhook instance this delivery was
    # created from, so the immediate post-commit send dispatches through
    # `Webhook.send()` exactly like the old fire-and-forget path.
    webhook: Webhook | None = field(default=None, repr=False, compare=False)
    exc_string: str | None = field(default=None, repr=False, compare=False)

    @property
    def key(self) -> str:
        return DELIVERY_KEY_TEMPLATE.format(delivery_id=self.id)

    @property
    def lock_key(self) -> str:
        return DELIVERY_LOCK_KEY_TEMPLATE.format(delivery_id=self.id)

    def to_hash(self) -> dict[str, Any]:
        return {
            'id': self.id,
            'job_id': self.job_id,
            'job_status': self.job_status,
            'url': self.url,
            'method': self.method,
            'timeout': str(self.timeout),
            'headers': json.dumps(self.headers) if self.headers is not None else '',
            'payload': json.dumps(self.payload) if self.payload is not None else '',
            'state': self.state,
            'attempts': str(self.attempts),
            'created_at': self.created_at.isoformat(),
            'next_retry_at': str(self.next_retry_at),
            'last_error': self.last_error or '',
            'delivered_at': self.delivered_at.isoformat() if self.delivered_at else '',
        }

    @classmethod
    def from_hash(cls, data: dict[Any, Any]) -> WebhookDelivery:
        raw = {as_text(k): as_text(v) for k, v in data.items()}
        headers_raw = raw.get('headers') or ''
        payload_raw = raw.get('payload') or ''
        delivered_raw = raw.get('delivered_at') or ''
        return cls(
            id=raw['id'],
            job_id=raw['job_id'],
            job_status=raw['job_status'],
            url=raw['url'],
            method=raw['method'],
            timeout=int(raw.get('timeout') or 10),
            headers=json.loads(headers_raw) if headers_raw else None,
            payload=json.loads(payload_raw) if payload_raw else None,
            state=raw.get('state', STATE_PENDING),
            attempts=int(raw.get('attempts') or 0),
            created_at=datetime.fromisoformat(raw['created_at']),
            next_retry_at=float(raw.get('next_retry_at') or 0),
            last_error=raw.get('last_error') or None,
            delivered_at=datetime.fromisoformat(delivered_raw) if delivered_raw else None,
        )


def _delivery_id(job: Job, status: str, webhook_index: int, occurred_at: datetime) -> str:
    return f'{job.id}:{status}:{webhook_index}:{int(occurred_at.timestamp() * 1000)}'


def build_webhook_deliveries(
    job: Job, status: str | JobStatus, *, exc_string: str | None = None, occurred_at: datetime | None = None
) -> list[WebhookDelivery]:
    """Builds (without persisting) one pending delivery per webhook registered
    for the given terminal job status."""
    # JobStatus is a plain `str, Enum`: str(member) is 'JobStatus.FINISHED',
    # while the stored webhook statuses use the raw value ('finished').
    status_text = getattr(status, 'value', status)
    occurred = occurred_at or job.ended_at or now()
    timestamp = current_timestamp()
    deliveries: list[WebhookDelivery] = []
    for index, webhook in enumerate(job.webhooks):
        if webhook.job_status != status:
            continue
        payload = webhook.get_payload(job, exc_string=exc_string) if webhook.method == 'POST' else None
        deliveries.append(
            WebhookDelivery(
                id=_delivery_id(job, status_text, index, occurred),
                job_id=job.id,
                job_status=status_text,
                url=webhook.url,
                method=webhook.method,
                headers=dict(webhook.headers) if webhook.headers else None,
                timeout=webhook.timeout,
                payload=payload,
                next_retry_at=timestamp,
                webhook=webhook,
                exc_string=exc_string,
            )
        )
    return deliveries


def persist_webhook_deliveries(
    connection: Redis,
    job: Job,
    status: str | JobStatus,
    *,
    exc_string: str | None = None,
    pipeline: Pipeline | None = None,
    occurred_at: datetime | None = None,
) -> list[WebhookDelivery]:
    """Persists pending delivery records for the matching webhooks.

    Records are buffered into `pipeline` when given (so they land atomically
    with the job's terminal state) or written in their own transaction
    otherwise. The returned descriptors are ready to hand to
    `deliver_webhook_deliveries()`.
    """
    deliveries = build_webhook_deliveries(job, status, exc_string=exc_string, occurred_at=occurred_at)
    if not deliveries:
        return deliveries

    script = connection.register_script(CREATE_DELIVERY_SCRIPT)
    owns_pipeline = pipeline is None
    pipe = pipeline or connection.pipeline()
    for delivery in deliveries:
        script(
            keys=[delivery.key, PENDING_DELIVERIES_KEY],
            args=[
                delivery.id,
                delivery.next_retry_at,
                DEFAULT_WEBHOOK_DELIVERY_MAX_AGE,
                *flatten_hash(delivery.to_hash()),
            ],
            client=pipe,
        )
    if owns_pipeline:
        pipe.execute()
    return deliveries


def flatten_hash(mapping: dict[str, Any]) -> list[str]:
    """Flattens a hash dict into [field1, value1, ...] for HSET-in-Lua."""
    flat: list[str] = []
    for key, value in mapping.items():
        flat.append(key)
        flat.append('' if value is None else str(value))
    return flat


def _write_delivery_hash(pipe: Pipeline, delivery: WebhookDelivery) -> None:
    # redis-py's Pipeline hset stubs type the mapping key as the full response
    # union; the plain str->str mapping is valid at runtime.
    pipe.hset(delivery.key, mapping=cast(Any, delivery.to_hash()))


def _mark_delivered(pipe: Pipeline, delivery: WebhookDelivery) -> None:
    delivery.state = STATE_DELIVERED
    delivery.attempts += 1
    delivery.delivered_at = now()
    _write_delivery_hash(pipe, delivery)
    pipe.zrem(PENDING_DELIVERIES_KEY, delivery.id)
    pipe.expire(delivery.key, DEFAULT_WEBHOOK_DELIVERED_TTL)
    pipe.delete(delivery.lock_key)


def _reschedule_or_dead(pipe: Pipeline, delivery: WebhookDelivery, error: str) -> None:
    delivery.attempts += 1
    delivery.last_error = error[:_MAX_ERROR_LENGTH]
    age = (now() - delivery.created_at).total_seconds()

    if age >= DEFAULT_WEBHOOK_DELIVERY_MAX_AGE:
        delivery.state = STATE_DEAD
        _write_delivery_hash(pipe, delivery)
        pipe.zrem(PENDING_DELIVERIES_KEY, delivery.id)
        pipe.expire(delivery.key, DEFAULT_WEBHOOK_DELIVERED_TTL)
        logger.error(
            'Webhook delivery %s to %s marked dead after %d failed attempt(s): %s',
            delivery.id,
            delivery.url,
            delivery.attempts,
            delivery.last_error,
        )
    else:
        delivery.next_retry_at = current_timestamp() + _backoff_delay(delivery.attempts)
        _write_delivery_hash(pipe, delivery)
        pipe.zadd(PENDING_DELIVERIES_KEY, {delivery.id: delivery.next_retry_at})
        pipe.expire(delivery.key, DEFAULT_WEBHOOK_DELIVERY_MAX_AGE)
        logger.warning(
            'Webhook delivery %s to %s failed (attempt %d), retry due at %s: %s',
            delivery.id,
            delivery.url,
            delivery.attempts,
            delivery.next_retry_at,
            delivery.last_error,
        )
    pipe.delete(delivery.lock_key)


def _perform_send(delivery: WebhookDelivery, job: Job | None) -> bool:
    # Immediate post-commit dispatch goes through the Webhook instance so the
    # call is indistinguishable from the old fire-and-forget path. Retries load
    # deliveries from Redis (no Webhook instance, possibly no job anymore) and
    # resend from the snapshotted fields.
    if job is not None and delivery.webhook is not None:
        return delivery.webhook.send(job, exc_string=delivery.exc_string, idempotency_key=delivery.id)
    return send_webhook(
        url=delivery.url,
        method=delivery.method,
        headers=delivery.headers,
        timeout=delivery.timeout,
        payload=delivery.payload,
        idempotency_key=delivery.id,
        log_context=f'job {delivery.job_id}',
    )


def attempt_webhook_delivery(connection: Redis, delivery: WebhookDelivery, *, job: Job | None = None) -> bool:
    """Sends one delivery, claiming it and honoring its persisted state.

    Returns True when the delivery is marked delivered, False otherwise (not
    due, claimed by someone else, send failed, marked dead). Never raises:
    webhook problems must never abort a job or a reconciliation pass.
    """
    claimed = connection.set(delivery.lock_key, delivery.id, nx=True, ex=_claim_lock_ttl(delivery.timeout))
    if not claimed:
        return False

    try:
        raw = connection.hgetall(delivery.key)
        if not raw:
            # Index entry without a hash (e.g. record expired): drop the member.
            connection.zrem(PENDING_DELIVERIES_KEY, delivery.id)
            return False
        persisted = WebhookDelivery.from_hash(raw)
        if persisted.state != STATE_PENDING:
            connection.zrem(PENDING_DELIVERIES_KEY, persisted.id)
            return False

        # Use the persisted attempt counters / timestamps from here on.
        persisted.webhook = delivery.webhook
        persisted.exc_string = delivery.exc_string

        ok = _perform_send(persisted, job)
        with connection.pipeline() as pipe:
            if ok:
                _mark_delivered(pipe, persisted)
            else:
                _reschedule_or_dead(pipe, persisted, 'request failed (see rq.webhook log for details)')
            pipe.execute()
        delivery.state = persisted.state
        delivery.attempts = persisted.attempts
        delivery.next_retry_at = persisted.next_retry_at
        return ok
    except Exception:
        logger.exception('Webhook delivery %s raised while being sent', delivery.id)
        connection.delete(delivery.lock_key)
        return False


def deliver_webhook_deliveries(connection: Redis, deliveries: list[WebhookDelivery], *, job: Job | None = None) -> None:
    """Immediately attempts each freshly-persisted delivery once. Never raises."""
    for delivery in deliveries:
        attempt_webhook_delivery(connection, delivery, job=job)


def reconcile_pending_webhooks(connection: Redis, *, batch_size: int = DEFAULT_WEBHOOK_RECONCILE_BATCH_SIZE) -> int:
    """Retries due pending webhook deliveries until they converge.

    Designed to be called repeatedly (workers run it on an interval) and to be
    safe under concurrency: a global short-lived lock serializes reconciliation
    passes while per-delivery claim locks prevent double sends. Returns the
    number of due deliveries examined. Never raises.
    """
    acquired = connection.set(RECONCILE_LOCK_KEY, '1', nx=True, ex=DEFAULT_WEBHOOK_RECONCILE_LOCK_TTL)
    if not acquired:
        return 0

    examined = 0
    try:
        due_ids = connection.zrangebyscore(
            PENDING_DELIVERIES_KEY, min='-inf', max=current_timestamp(), start=0, num=batch_size
        )
        examined = len(due_ids)
        for raw_id in due_ids:
            delivery_id = as_text(cast('bytes | str', raw_id))
            try:
                raw = connection.hgetall(DELIVERY_KEY_TEMPLATE.format(delivery_id=delivery_id))
                if not raw:
                    # Hash expired/removed but the index entry lingers.
                    connection.zrem(PENDING_DELIVERIES_KEY, delivery_id)
                    continue
                delivery = WebhookDelivery.from_hash(raw)
                if delivery.state != STATE_PENDING:
                    connection.zrem(PENDING_DELIVERIES_KEY, delivery_id)
                    continue
                attempt_webhook_delivery(connection, delivery)
            except Exception:
                logger.exception('Reconciling webhook delivery %s raised', delivery_id)
    except Exception:
        logger.exception('Webhook reconciliation pass raised')
    finally:
        connection.delete(RECONCILE_LOCK_KEY)
    return examined
