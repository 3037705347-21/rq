from __future__ import annotations

import json
import re
from typing import Any

from redis import Redis
from redis.client import Pipeline

#: Plan IDs are embedded verbatim in the Redis key, restrict them to a safe charset
PLAN_ID_PATTERN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.:-]*$')


def get_plan_key(plan_id: str) -> str:
    """Get the Redis hash key holding the managed cron definitions of a plan.

    Each plan owns its own key, so reconciling one plan never touches the tasks
    of another plan or any ad-hoc cron jobs.

    Args:
        plan_id: Identifier of the cron plan (e.g. 'default')

    Raises:
        ValueError: If plan_id is empty or contains unsupported characters
    """
    if not isinstance(plan_id, str) or not PLAN_ID_PATTERN.match(plan_id):
        raise ValueError(
            f'Invalid plan_id {plan_id!r}: only letters, digits and "_.:-" are allowed '
            '(must start with a letter or digit)'
        )
    return f'rq:cron:plan:{plan_id}'


def get_definitions(connection: Redis, plan_id: str) -> dict[str, str]:
    """Return the managed definitions of a plan as ``{name: raw canonical JSON}``.

    Missing plans yield an empty dict. Values are returned as the raw canonical
    JSON strings written by :func:`apply_diff`, so callers can compare them
    byte-for-byte.
    """
    raw = connection.hgetall(get_plan_key(plan_id))
    return {
        name.decode('utf-8') if isinstance(name, bytes) else name: value.decode('utf-8')
        if isinstance(value, bytes)
        else value
        for name, value in raw.items()
    }


def parse_definition(raw_value: str) -> dict[str, Any]:
    """Decode a single canonical definition JSON string into a dict."""
    return json.loads(raw_value)


def apply_diff(
    connection: Redis,
    plan_id: str,
    definitions: dict[str, str],
    removed: list[str],
    pipeline: Pipeline | None = None,
) -> bool:
    """Apply a reconciled diff to the plan's definitions hash atomically.

    Args:
        connection: Redis connection to use
        plan_id: Identifier of the cron plan
        definitions: Mapping of name -> canonical JSON for added/changed entries
        removed: Names to remove from the hash
        pipeline: Optional pipeline to enqueue the writes into. When None, the
            writes are executed in a single MULTI/EXEC transaction.

    Returns:
        True if any write was issued, False when both inputs are empty (no-op).
    """
    if not definitions and not removed:
        return False

    key = get_plan_key(plan_id)
    owns_pipeline = pipeline is None
    buf: Pipeline = connection.pipeline() if pipeline is None else pipeline

    # One hset per field keeps everything in the same transaction while staying
    # compatible with the redis-py hset stubs across supported versions.
    for name, raw_value in definitions.items():
        buf.hset(key, name, raw_value)
    if removed:
        buf.hdel(key, *removed)

    if owns_pipeline:
        buf.execute()
    return True
