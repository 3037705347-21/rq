from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse
from urllib.request import Request, urlopen

if TYPE_CHECKING:
    from .job import Job

logger = logging.getLogger('rq.webhook')

IDEMPOTENCY_KEY_HEADER = 'Idempotency-Key'


@dataclass
class Webhook:
    """A pure-data description of an HTTP request to perform when a job
    reaches a terminal state (``finished`` or ``failed``).

    Unlike callbacks, webhooks are JSON-serializable and don't require an
    importable function. Send failures are logged and never raised, so an
    unreachable endpoint cannot fail the job.
    """

    url: str
    job_status: Literal['finished', 'failed']
    method: Literal['GET', 'POST']
    headers: dict[str, str] | None
    timeout: int

    def __init__(
        self,
        url: str,
        job_status: Literal['finished', 'failed'],
        *,
        method: Literal['GET', 'POST'] = 'GET',
        headers: dict[str, str] | None = None,
        timeout: int = 10,
    ) -> None:
        parsed = urlparse(url) if isinstance(url, str) else None
        if parsed is None or parsed.scheme not in ('http', 'https') or not parsed.netloc:
            raise ValueError(f'url must be an http:// or https:// URL, got {url!r}')

        if job_status not in ('finished', 'failed'):
            raise ValueError(f"job_status must be 'finished' or 'failed', got {job_status!r}")

        if method not in ('GET', 'POST'):
            raise ValueError(f"method must be 'GET' or 'POST', got {method!r}")

        if headers and not isinstance(headers, dict):
            raise TypeError(f'headers must be a dict or None, got {type(headers).__name__}')

        if not isinstance(timeout, int) or timeout <= 0:
            raise ValueError(f'timeout must be a positive integer, got {timeout!r}')

        self.url = url
        self.job_status = job_status
        self.method = method
        self.headers = headers
        self.timeout = timeout

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Webhook:
        return cls(
            data['url'],
            data['job_status'],
            method=data.get('method', 'GET'),
            headers=data.get('headers'),
            timeout=data.get('timeout', 10),
        )

    def get_payload(self, job: Job, *, exc_string: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            'job_id': job.id,
            'func_name': job.func_name,
            'status': self.job_status,
            'enqueued_at': job.enqueued_at.isoformat() if job.enqueued_at else None,
            'ended_at': job.ended_at.isoformat() if job.ended_at else None,
        }
        if self.job_status == 'failed':
            payload['exc_info'] = exc_string
        return payload

    def send(self, job: Job, *, exc_string: str | None = None, idempotency_key: str | None = None) -> bool:
        """Performs the HTTP request. Errors are logged, never raised.

        Returns `True` when the endpoint accepted the request (2xx/3xx) and
        `False` when sending failed.
        """
        payload = None if self.method == 'GET' else self.get_payload(job, exc_string=exc_string)
        return send_webhook(
            url=self.url,
            method=self.method,
            headers=self.headers,
            timeout=self.timeout,
            payload=payload,
            idempotency_key=idempotency_key,
            log_context=f'job {job.id}',
        )


def perform_webhook_request(
    *,
    url: str,
    method: str,
    headers: dict[str, str] | None,
    timeout: int,
    payload: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
) -> None:
    """Performs a webhook HTTP request from already-serialized data.

    Unlike `Webhook.send()`, this doesn't need a `Job` instance, so it can
    resend deliveries long after the job itself has expired. Raises on any
    failure (connection error, timeout, HTTP error); callers decide how to
    record the outcome.
    """
    request_headers = dict(headers or {})
    if idempotency_key:
        request_headers[IDEMPOTENCY_KEY_HEADER] = idempotency_key

    if method == 'GET':
        request = Request(url, headers=request_headers, method='GET')
    else:
        body = json.dumps(payload).encode('utf-8') if payload is not None else b''
        request = Request(
            url, data=body, headers={'Content-Type': 'application/json', **request_headers}, method='POST'
        )
    with urlopen(request, timeout=timeout):
        pass


def send_webhook(
    *,
    url: str,
    method: str,
    headers: dict[str, str] | None,
    timeout: int,
    payload: dict[str, Any] | None = None,
    idempotency_key: str | None = None,
    log_context: str = '',
) -> bool:
    """Performs a webhook request, returning whether it succeeded.

    Errors are logged and never raised so a dead endpoint can never abort a
    job or a reconciliation pass.
    """
    try:
        perform_webhook_request(
            url=url,
            method=method,
            headers=headers,
            timeout=timeout,
            payload=payload,
            idempotency_key=idempotency_key,
        )
    except Exception:
        logger.warning(
            'Failed to send webhook to %s%s', url, f' for {log_context}' if log_context else '', exc_info=True
        )
        return False
    return True
