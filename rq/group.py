from __future__ import annotations

import json
from base64 import b64decode, b64encode
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from redis import Redis
from redis.client import Pipeline

from . import Queue
from .exceptions import InvalidRosterError, NoSuchGroupError
from .job import Job
from .queue import EnqueueData
from .results import Result
from .serializers import resolve_serializer
from .utils import as_text, now


class GroupResult:
    """The single, complete aggregated result of a Group.

    A GroupResult is tied to the exact member roster that produced it; re-running
    the settlement for the same roster returns the same GroupResult instead of
    producing a second copy.
    """

    def __init__(
        self,
        group_name: str,
        roster: list[str],
        results: dict[str, Any],
        connection: Redis,
        created_at: datetime | None = None,
        serializer=None,
    ):
        self.group_name = group_name
        self.roster = roster
        self.results = results
        self.connection = connection
        self.created_at = created_at if created_at else now()
        self.serializer = resolve_serializer(serializer)

    def __repr__(self):
        return f'GroupResult(group={self.group_name}, members={len(self.roster)})'

    def __eq__(self, other):
        try:
            return self.group_name == other.group_name and self.roster == other.roster
        except AttributeError:
            return False

    def __hash__(self):
        return hash((self.group_name, tuple(self.roster)))

    def serialize(self) -> dict[str, str]:
        return {
            'roster': json.dumps(self.roster),
            'results': b64encode(self.serializer.dumps(self.results)).decode(),
            'created_at': str(self.created_at.timestamp()),
        }

    def save(self, pipeline: Pipeline, ttl: int | None = None):
        """Write this ledger as the one group result.

        The hash is deleted first so fields left over from a previous roster are
        fully replaced.
        """
        key = Group.result_key_for(self.group_name)
        pipeline.delete(key)
        pipeline.hset(key, mapping=self.serialize())
        if ttl is not None:
            if ttl == -1:
                pipeline.persist(key)
            else:
                pipeline.expire(key, ttl)

    @classmethod
    def restore(cls, group_name: str, payload: dict, connection: Redis, serializer=None) -> GroupResult:
        """Create a GroupResult from a Redis hash payload."""
        data = {as_text(k): v for k, v in payload.items()}
        resolved = resolve_serializer(serializer)
        roster = json.loads(data['roster'])
        results = resolved.loads(b64decode(data['results']))
        created_at = datetime.fromtimestamp(float(data['created_at']), tz=timezone.utc)
        return cls(
            group_name=group_name,
            roster=roster,
            results=results,
            connection=connection,
            created_at=created_at,
            serializer=resolved,
        )

    @classmethod
    def fetch(cls, group: Group, serializer=None) -> GroupResult | None:
        """Fetch the group's committed result, or None if none has been committed."""
        payload = group.connection.hgetall(group.result_key)
        if not payload:
            return None
        return cls.restore(group.name, payload, connection=group.connection, serializer=serializer)


class Group:
    """A Group is a container for tracking multiple jobs with a single identifier."""

    REDIS_GROUP_NAME_PREFIX = 'rq:group:'
    REDIS_GROUP_KEY = 'rq:groups'
    REDIS_GROUP_ROSTER_SUFFIX = ':roster'
    REDIS_GROUP_RESULT_SUFFIX = ':result'

    def __init__(self, connection: Redis, name: str | None = None):
        self.name = name if name else str(uuid4().hex)
        self.connection = connection
        self.key = f'{self.REDIS_GROUP_NAME_PREFIX}{self.name}'

    def __repr__(self):
        return f'Group(id={self.name})'

    @property
    def roster_key(self) -> str:
        """Redis key of the set containing the roster-managed member IDs."""
        return self.key + self.REDIS_GROUP_ROSTER_SUFFIX

    @property
    def result_key(self) -> str:
        """Redis key of the hash containing the single committed group result."""
        return self.key + self.REDIS_GROUP_RESULT_SUFFIX

    def _add_jobs(self, jobs: Iterable[Job], pipeline: Pipeline):
        """Add jobs to the group"""
        pipeline.sadd(self.key, *[job.id for job in jobs])
        pipeline.sadd(self.REDIS_GROUP_KEY, self.name)
        pipeline.execute()

    def cleanup(self):
        """Delete jobs from the group's job registry that have been deleted or expired from Redis.
        We assume while running this that alive jobs have all been fetched from Redis in fetch_jobs method"""
        with self.connection.pipeline() as pipe:  # Use a new pipeline
            job_ids = [as_text(job) for job in list(self.connection.smembers(self.key))]
            if not job_ids:
                return
            expired_job_ids = []
            for job_id in job_ids:
                pipe.exists(Job.key_for(job_id))
            results = pipe.execute()

            for i, key_exists in enumerate(results):
                if not key_exists:
                    expired_job_ids.append(job_ids[i])
            if expired_job_ids:
                pipe.srem(self.key, *expired_job_ids)
                pipe.execute()

    def enqueue_many(self, queue: Queue, job_datas: Iterable[EnqueueData], pipeline: Pipeline | None = None):
        pipe = pipeline if pipeline else self.connection.pipeline()

        jobs = queue.enqueue_many(job_datas, group_id=self.name, pipeline=pipe)

        self._add_jobs(jobs, pipeline=pipe)

        if pipeline is None:
            pipe.execute()

        return jobs

    def get_jobs(self) -> list:
        """Retrieve list of job IDs from the group key in Redis"""
        self.cleanup()
        job_ids = [as_text(job) for job in self.connection.smembers(self.key)]
        return [job for job in Job.fetch_many(job_ids, self.connection) if job is not None]

    def delete_job(self, job_id: str, pipeline: Pipeline | None = None):
        pipe = pipeline if pipeline else self.connection.pipeline()
        pipe.srem(self.key, job_id)
        if pipeline is None:
            pipe.execute()

    def get_result(self, serializer=None) -> GroupResult | None:
        """Return the group's committed result, or None if none has been committed."""
        return GroupResult.fetch(self, serializer=serializer)

    def settle(
        self,
        members: Iterable[str | Job],
        serializer=None,
        result_ttl: int | None = -1,
    ) -> GroupResult | None:
        """Reconcile group membership against the given roster and, once every
        member has a successful result, commit a single complete group result.

        - Members are accepted as job IDs (str) or ``Job`` instances. The roster
          is normalized in order; an empty roster or a duplicate ID raises
          ``InvalidRosterError``.
        - If any roster member doesn't exist in Redis, ``InvalidRosterError`` is
          raised. Nothing is written in either error case.
        - If all members exist but any one has no successful result yet, returns
          ``None`` without writing anything. Call again later.
        - Once every member has a latest ``SUCCESSFUL`` result, membership is
          reconciled (missing members added, roster-dropped members removed;
          members added manually outside the roster are left untouched) and one
          ``GroupResult`` is committed, all in a single atomic pipeline.
        - Re-running with the same roster returns the already-committed
          ``GroupResult`` unchanged.
        """
        # 1. Normalize and validate the roster (local checks, no writes)
        roster_ids: list[str] = []
        for member in members:
            member_id = member.id if isinstance(member, Job) else as_text(member)
            roster_ids.append(member_id)

        if not roster_ids:
            raise InvalidRosterError('Roster must contain at least one member.')
        if len(roster_ids) != len(set(roster_ids)):
            raise InvalidRosterError(f'Roster contains duplicate member IDs: {roster_ids}')

        # 2. Idempotent fast path: a result for this exact roster already exists
        existing = GroupResult.fetch(self, serializer=serializer)
        if existing is not None and existing.roster == roster_ids:
            return existing

        # 3. Every roster member must exist in Redis
        fetched_jobs = Job.fetch_many(roster_ids, connection=self.connection, serializer=serializer)
        jobs: list[Job] = []
        for job_id, job in zip(roster_ids, fetched_jobs):
            if job is None:
                raise InvalidRosterError(f'Roster references jobs that do not exist: {job_id}')
            jobs.append(job)

        ledger_serializer = resolve_serializer(serializer) if serializer else jobs[0].serializer

        # 4. Read each member's real latest result (batched, read-only)
        with self.connection.pipeline() as pipe:
            for job in jobs:
                pipe.xrevrange(Result.get_key(job.id), '+', '-', count=1)
            responses = pipe.execute()

        aggregated: dict[str, Any] = {}
        for job, response in zip(jobs, responses):
            if not response:
                return None
            result_id, payload = response[0]
            result = Result.restore(
                job.id,
                result_id.decode(),
                payload,
                connection=self.connection,
                serializer=job.serializer,
            )
            if result.type != Result.Type.SUCCESSFUL:
                return None
            aggregated[job.id] = result.return_value

        # 5. Reconcile membership and commit the ledger atomically
        current_members = {as_text(member) for member in self.connection.smembers(self.key)}
        old_roster = {as_text(member) for member in self.connection.smembers(self.roster_key)}
        new_roster = set(roster_ids)

        members_to_add = new_roster - current_members
        members_to_remove = old_roster - new_roster
        roster_to_add = new_roster - old_roster
        roster_to_remove = old_roster - new_roster

        group_result = GroupResult(
            group_name=self.name,
            roster=roster_ids,
            results=aggregated,
            connection=self.connection,
            serializer=ledger_serializer,
        )

        with self.connection.pipeline() as pipe:
            if members_to_add:
                pipe.sadd(self.key, *members_to_add)
            if members_to_remove:
                pipe.srem(self.key, *members_to_remove)
            if roster_to_add:
                pipe.sadd(self.roster_key, *roster_to_add)
            if roster_to_remove:
                pipe.srem(self.roster_key, *roster_to_remove)
            group_result.save(pipe, ttl=result_ttl)
            pipe.sadd(self.REDIS_GROUP_KEY, self.name)
            pipe.execute()

        return group_result

    @classmethod
    def create(cls, connection: Redis, name: str | None = None):
        return cls(name=name, connection=connection)

    @classmethod
    def fetch(cls, name: str, connection: Redis):
        """Fetch an existing group from Redis"""
        group = cls(name=name, connection=connection)
        if not connection.exists(Group.get_key(group.name)):
            raise NoSuchGroupError
        return group

    @classmethod
    def all(cls, connection: Redis) -> list[Group]:
        "Returns an iterable of all Groups."
        group_keys = [as_text(key) for key in connection.smembers(cls.REDIS_GROUP_KEY)]
        groups = []
        for key in group_keys:
            try:
                groups.append(cls.fetch(key, connection=connection))
            except NoSuchGroupError:
                connection.srem(cls.REDIS_GROUP_KEY, key)
        return groups

    @classmethod
    def get_key(cls, name: str) -> str:
        """Return the Redis key of the set containing a group's jobs"""
        return cls.REDIS_GROUP_NAME_PREFIX + name

    @classmethod
    def result_key_for(cls, name: str) -> str:
        """Return the Redis key of the hash containing a group's committed result."""
        return cls.REDIS_GROUP_NAME_PREFIX + name + cls.REDIS_GROUP_RESULT_SUFFIX

    @classmethod
    def clean_registries(cls, connection: Redis):
        """Loop through groups and delete those that have been deleted.
        If group still has jobs in its registry, delete those that have expired"""
        groups = Group.all(connection=connection)
        with connection.pipeline() as p:
            # Remove expired jobs from groups
            for group in groups:
                group.cleanup()
            p.execute()
            # Remove empty groups from group registry
            for group in groups:
                p.exists(group.key)
            results = p.execute()
            expired_group_ids = []
            for i, key_exists in enumerate(results):
                if not key_exists:
                    expired_group_ids.append(groups[i].name)
            if expired_group_ids:
                p.srem(cls.REDIS_GROUP_KEY, *expired_group_ids)
            p.execute()
