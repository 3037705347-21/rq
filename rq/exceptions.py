class NoSuchJobError(Exception):
    """Raised when a job cannot be found in Redis, e.g. by ``Job.fetch()`` for an unknown or expired job ID."""


class NoSuchGroupError(Exception):
    """Raised by ``Group.fetch()`` when no group with the given name exists in Redis."""


class DeserializationError(Exception):
    """Raised when a job's stored data (function, args and kwargs) cannot be loaded by its serializer."""


class InvalidJobDependency(Exception):
    """Not currently raised by RQ."""


class DuplicateJobError(Exception):
    """Raised when enqueueing a job whose ID already exists in Redis."""


class InvalidJobOperationError(Exception):
    """Not currently raised by RQ; see :class:`InvalidJobOperation`."""


class InvalidJobOperation(Exception):
    """Raised when an operation does not apply to the job's current state.

    For example, requeueing a job that is no longer in the registry, or sending a
    stop command for a job that is not currently executing.
    """


class DequeueTimeout(Exception):
    """Raised when a blocking dequeue times out before any job arrives on the queues."""


class ShutDownImminentException(BaseException):
    """Raised in the work horse to cancel the running job before a forced stop.

    ``extra_info`` holds details of the interrupted stack frame.
    """

    # Inherit from BaseException as this is used specifically as a
    # 'shutdown' signal and should not be caught by except Exception.
    def __init__(self, msg, extra_info):
        self.extra_info = extra_info
        super().__init__(msg)


class TimeoutFormatError(Exception):
    """Raised when a timeout is neither an integer nor a string such as ``"1h"`` or ``"23m"``."""


class AbandonedJobError(Exception):
    """Passed to failure callbacks and exception handlers when a job was left in the
    started state because its worker died while running it.
    """


class SchedulerNotFound(Exception):
    """Raised when no scheduler with the given name is registered."""


class DuplicateSchedulerError(Exception):
    """Raised when registering a cron scheduler whose name is already registered."""


class StopRequested(Exception):
    """Raised inside a worker to stop the work loop, e.g. after a stop signal or
    when the worker is suspended in burst mode.
    """


class PayloadError(Exception):
    """Base class for errors related to offloaded job payloads stored in the content store."""


class PayloadNotFoundError(PayloadError):
    """Raised when a job references an offloaded payload that does not exist in the content store.

    The job itself is left untouched; this indicates a dangling reference that should be
    reported, since the original inline payload is no longer present in the job hash.
    """


class PayloadIntegrityError(PayloadError):
    """Raised when an offloaded payload's checksum does not match the reference,
    or when a payload reference is malformed.
    """


class PayloadUnreadableError(PayloadError):
    """Raised by the migration when a job's inline payload cannot be decoded
    (zlib envelope) or loaded by its serializer. The job is deliberately left as-is.
    """
