"""Declarative cron plans and their reconciliation against live state.

A *cron plan* is a reviewable, version-controlled list of desired cron jobs::

    # cron_plan.py
    from rq.cron_plan import define
    from myapp.tasks import cleanup, send_report

    define(cleanup, queue_name='maintenance', interval=600)
    define(send_report, queue_name='reports', cron='0 9 * * 1')

Reconciling a plan with :func:`reconcile` makes the live managed definitions
match the plan: new and changed entries take effect, entries removed from the
plan are deactivated, and anything outside the plan (ad-hoc jobs, other plans)
is left untouched. Validation covers the whole batch and happens before any
write, so an invalid plan is rejected atomically; applying the same plan twice
converges to identical state (the second apply is a no-op).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from croniter import croniter
from redis import Redis
from redis.client import Pipeline

from . import cron_plan_registry
from .cron import CronJob, load_config_module
from .defaults import DEFAULT_RESULT_TTL
from .exceptions import InvalidCronPlanError
from .utils import import_attribute
from .webhook import Webhook

#: Default namespace for managed definitions
DEFAULT_PLAN_ID = 'default'

#: Version of the serialized definition format
DEFINITION_VERSION = 1

logger = logging.getLogger(__name__)


@dataclass
class CronDefinition:
    """One desired cron job within a cron plan.

    Only declarative fields are stored; runtime timing (last/next enqueue) lives
    in the schedulers and is intentionally excluded so that re-applying the same
    plan always converges.
    """

    name: str
    func_name: str
    queue_name: str
    interval: int | None = None
    cron: str | None = None
    args: tuple = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    job_timeout: int | None = None
    result_ttl: int = DEFAULT_RESULT_TTL
    ttl: int | None = None
    failure_ttl: int | None = None
    meta: dict[str, Any] | None = None
    webhooks: list[Webhook] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable dict containing only declarative fields."""
        data: dict[str, Any] = {
            'version': DEFINITION_VERSION,
            'name': self.name,
            'func_name': self.func_name,
            'queue_name': self.queue_name,
            'args': list(self.args),
            'kwargs': self.kwargs,
            'interval': self.interval,
            'cron': self.cron,
            'result_ttl': self.result_ttl,
        }
        if self.job_timeout is not None:
            data['job_timeout'] = self.job_timeout
        if self.ttl is not None:
            data['ttl'] = self.ttl
        if self.failure_ttl is not None:
            data['failure_ttl'] = self.failure_ttl
        if self.meta is not None:
            data['meta'] = self.meta
        if self.webhooks:
            data['webhooks'] = [webhook.to_dict() for webhook in self.webhooks]
        return data

    def canonical_json(self) -> str:
        """Stable JSON representation used as the stored value and change key."""
        return json.dumps(self.to_dict(), sort_keys=True, separators=(',', ':'))

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CronDefinition:
        """Rebuild a definition from its serialized dict."""
        version = data.get('version', DEFINITION_VERSION)
        if version != DEFINITION_VERSION:
            raise ValueError(f'Unsupported cron definition version: {version!r}')

        webhooks_data = data.get('webhooks')
        webhooks = [Webhook.from_dict(item) for item in webhooks_data] if webhooks_data else None
        return cls(
            name=data['name'],
            func_name=data['func_name'],
            queue_name=data['queue_name'],
            interval=data.get('interval'),
            cron=data.get('cron'),
            args=tuple(data.get('args', ())),
            kwargs=data.get('kwargs', {}),
            job_timeout=data.get('job_timeout'),
            result_ttl=data.get('result_ttl', DEFAULT_RESULT_TTL),
            ttl=data.get('ttl'),
            failure_ttl=data.get('failure_ttl'),
            meta=data.get('meta'),
            webhooks=webhooks,
        )

    def to_cron_job(self, resolve_func: bool = True) -> CronJob:
        """Build an executable (or func-less, monitoring-only) CronJob.

        Args:
            resolve_func: When True (default), import the function by its dotted
                path so the job can be enqueued. When False, a func-less CronJob
                is returned, matching `CronJob.from_dict` semantics.
        """
        func = import_attribute(self.func_name) if resolve_func else None
        return CronJob(
            queue_name=self.queue_name,
            func=func,
            func_name=None if func else self.func_name,
            args=self.args,
            kwargs=self.kwargs,
            interval=self.interval,
            cron=self.cron,
            job_timeout=self.job_timeout,
            result_ttl=self.result_ttl,
            ttl=self.ttl,
            failure_ttl=self.failure_ttl,
            meta=self.meta,
            webhooks=self.webhooks,
            name=self.name,
        )


def _build_definition(entry: dict[str, Any], index: int) -> tuple[CronDefinition | None, list[str], str]:
    """Validate a single plan entry and build its definition.

    Returns (definition, errors, resolved_name); definition is None when any
    error was found, and resolved_name is the entry's effective name (explicit
    or derived from the function) even when other validation failed, so that
    duplicates can be detected independently of other errors.
    """
    errors: list[str] = []
    label = f'entry {index}'

    func = entry.get('func')
    func_name = entry.get('func_name')
    if func is not None:
        if not callable(func):
            errors.append(f'{label}: func must be callable, got {type(func).__name__}')
            func_name = None
        else:
            func_name = f'{func.__module__}.{func.__name__}'
    elif isinstance(func_name, str) and func_name:
        try:
            import_attribute(func_name)
        except (ValueError, ImportError, AttributeError):
            errors.append(f'{label}: cannot import function {func_name!r}')
            func_name = None
    else:
        errors.append(f'{label}: either func or func_name must be provided')
        func_name = None

    name = entry.get('name') or (func_name or '')
    if not isinstance(name, str) or not name:
        errors.append(f'{label}: name must be a non-empty string')
        name = ''
    else:
        label = f'entry {index} ({name!r})'

    queue_name = entry.get('queue_name')
    if not isinstance(queue_name, str) or not queue_name:
        errors.append(f'{label}: queue_name must be a non-empty string')

    interval = entry.get('interval')
    cron = entry.get('cron')
    if interval is not None and cron is not None:
        errors.append(f'{label}: cannot specify both interval and cron')
    elif interval is None and cron is None:
        errors.append(f'{label}: must specify either interval or cron')
    if interval is not None:
        if isinstance(interval, bool) or not isinstance(interval, int) or interval <= 0:
            errors.append(f'{label}: interval must be a positive integer number of seconds, got {interval!r}')
    if cron is not None:
        if not isinstance(cron, str) or not cron:
            errors.append(f'{label}: cron must be a non-empty string')
        else:
            try:
                croniter(cron)
            except (ValueError, KeyError) as exc:
                errors.append(f'{label}: invalid cron expression {cron!r}: {exc}')

    raw_args = entry.get('args')
    args: tuple = ()
    if raw_args is not None:
        if not isinstance(raw_args, (tuple, list)):
            errors.append(f'{label}: args must be a list or tuple, got {type(raw_args).__name__}')
        else:
            args = tuple(raw_args)

    raw_kwargs = entry.get('kwargs')
    kwargs: dict[str, Any] = {}
    if raw_kwargs is not None:
        if not isinstance(raw_kwargs, dict):
            errors.append(f'{label}: kwargs must be a dict, got {type(raw_kwargs).__name__}')
        else:
            kwargs = raw_kwargs

    meta = entry.get('meta')
    if meta is not None and not isinstance(meta, dict):
        errors.append(f'{label}: meta must be a dict, got {type(meta).__name__}')

    for field_name, value in (('args', args), ('kwargs', kwargs), ('meta', meta)):
        if value:
            try:
                json.dumps(value)
            except (TypeError, ValueError) as exc:
                errors.append(f'{label}: {field_name} must be JSON serializable: {exc}')

    webhooks = entry.get('webhooks')
    if webhooks is not None and (
        not isinstance(webhooks, Sequence) or not all(isinstance(webhook, Webhook) for webhook in webhooks)
    ):
        errors.append(f'{label}: webhooks must be a sequence of Webhook instances')

    if errors:
        return None, errors, name

    assert func_name is not None and name and queue_name is not None
    definition = CronDefinition(
        name=name,
        func_name=func_name,
        queue_name=queue_name,
        interval=interval,
        cron=cron,
        args=args,
        kwargs=kwargs,
        job_timeout=entry.get('job_timeout'),
        result_ttl=entry.get('result_ttl', DEFAULT_RESULT_TTL),
        ttl=entry.get('ttl'),
        failure_ttl=entry.get('failure_ttl'),
        meta=meta,
        webhooks=list(webhooks) if webhooks else None,
    )
    return definition, [], name


class CronPlan:
    """A validated, reviewable list of desired :class:`CronDefinition` entries."""

    def __init__(self, entries: Sequence[dict[str, Any]] | None = None):
        self.definitions: list[CronDefinition] = []
        errors: list[str] = []
        built: list[tuple[int, str, CronDefinition | None]] = []

        for index, entry in enumerate(entries or (), start=1):
            definition, entry_errors, resolved_name = _build_definition(entry, index)
            errors.extend(entry_errors)
            built.append((index, resolved_name, definition))

        # Duplicate detection is independent of per-entry validation so that a
        # name clash is reported even when one of the entries has other errors.
        name_counts: dict[str, int] = {}
        for _, resolved_name, _ in built:
            if resolved_name:
                name_counts[resolved_name] = name_counts.get(resolved_name, 0) + 1

        for index, resolved_name, definition in built:
            if resolved_name and name_counts[resolved_name] > 1:
                errors.append(
                    f'entry {index} ({resolved_name!r}): duplicate cron job name; names must be unique within a plan'
                )
            elif definition is not None:
                self.definitions.append(definition)

        if errors:
            raise InvalidCronPlanError(errors)

    @classmethod
    def load(cls, config_path: str) -> CronPlan:
        """Load a plan from a Python file or dotted module using `define()`."""
        global _plan_definition_registry
        _plan_definition_registry = []
        try:
            load_config_module(config_path)
            entries = _plan_definition_registry[:]
        finally:
            _plan_definition_registry = []
        return cls(entries)

    def get_definition(self, name: str) -> CronDefinition | None:
        for definition in self.definitions:
            if definition.name == name:
                return definition
        return None

    def __len__(self) -> int:
        return len(self.definitions)

    def __iter__(self):
        return iter(self.definitions)


# Global registry populated by define() while a plan file is being imported
_plan_definition_registry: list[dict[str, Any]] = []


def define(
    func: Callable,
    queue_name: str,
    args: tuple | None = None,
    kwargs: dict | None = None,
    interval: int | None = None,
    cron: str | None = None,
    job_timeout: int | None = None,
    result_ttl: int = DEFAULT_RESULT_TTL,
    ttl: int | None = None,
    failure_ttl: int | None = None,
    meta: dict | None = None,
    webhooks: Sequence[Webhook] | None = None,
    name: str = '',
) -> dict[str, Any]:
    """Declare a desired cron job from within a cron plan file.

    Mirrors `rq.cron.register`; the entry is collected while the plan file is
    imported by `CronPlan.load()` and validated as part of the whole batch.

    Example::

        from rq.cron_plan import define
        from my_app.tasks import my_func

        define(my_func, 'default', interval=60)
    """
    entry = {
        'func': func,
        'queue_name': queue_name,
        'args': args,
        'kwargs': kwargs,
        'interval': interval,
        'cron': cron,
        'job_timeout': job_timeout,
        'result_ttl': result_ttl,
        'ttl': ttl,
        'failure_ttl': failure_ttl,
        'meta': meta,
        'webhooks': webhooks,
        'name': name,
    }
    _plan_definition_registry.append(entry)
    logger.debug("Cron plan: declaring '%s' for queue %s", getattr(func, '__name__', func), queue_name)
    return entry


@dataclass
class ReconcileResult:
    """Outcome of reconciling one plan against its live managed definitions."""

    plan_id: str
    added: list[str]
    changed: list[str]
    removed: list[str]
    unchanged: list[str]

    @property
    def is_noop(self) -> bool:
        """True when live state already matched the plan (no writes were made)."""
        return not (self.added or self.changed or self.removed)

    def summary(self) -> str:
        lines = [
            f"Cron plan '{self.plan_id}': {len(self.added)} added, {len(self.changed)} changed, "
            f'{len(self.removed)} removed, {len(self.unchanged)} unchanged.'
        ]
        lines.extend(f'  + {name}' for name in self.added)
        lines.extend(f'  ~ {name}' for name in self.changed)
        lines.extend(f'  - {name}' for name in self.removed)
        return '\n'.join(lines)


def reconcile(
    plan: CronPlan,
    connection: Redis,
    plan_id: str = DEFAULT_PLAN_ID,
    pipeline: Pipeline | None = None,
) -> ReconcileResult:
    """Reconcile live managed cron definitions with a desired plan.

    The plan has already been fully validated when it was constructed, so this
    function only reads state and then applies the whole diff in one
    transaction. Entries not owned by the plan (other plan IDs, ad-hoc jobs
    registered directly with a scheduler) are never touched.

    Args:
        plan: The validated desired plan
        connection: Redis connection to use
        plan_id: Namespace of the managed definitions (default 'default')
        pipeline: Optional pipeline to enqueue the diff into (used by callers
            that want to apply the plan alongside other writes)

    Returns:
        ReconcileResult with the added/changed/removed/unchanged job names
    """
    desired = {definition.name: definition.canonical_json() for definition in plan.definitions}
    current = cron_plan_registry.get_definitions(connection, plan_id)

    added = sorted(name for name in desired if name not in current)
    removed = sorted(name for name in current if name not in desired)
    changed = sorted(name for name in desired if name in current and desired[name] != current[name])
    unchanged = sorted(name for name in desired if name in current and desired[name] == current[name])

    writes = {name: desired[name] for name in added + changed}
    cron_plan_registry.apply_diff(connection, plan_id, writes, removed, pipeline=pipeline)

    return ReconcileResult(
        plan_id=plan_id,
        added=added,
        changed=changed,
        removed=removed,
        unchanged=unchanged,
    )
