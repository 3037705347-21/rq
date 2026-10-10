DEFAULT_JOB_CLASS = 'rq.job.Job'
""" The path for the default Job class to use.
Defaults to the main `Job` class within the `rq.job` module
"""


DEFAULT_QUEUE_CLASS = 'rq.Queue'
""" The path for the default Queue class to use.
Defaults to the main `Queue` class within the `rq.queue` module
"""


DEFAULT_WORKER_CLASS = 'rq.Worker'
""" The path for the default Worker class to use.
Defaults to the main `Worker` class within the `rq.worker` module
"""


DEFAULT_SERIALIZER_CLASS = 'rq.serializers.DefaultSerializer'
""" The path for the default Serializer class to use.
Defaults to the main `DefaultSerializer` class within the `rq.serializers` module
"""


DEFAULT_CONNECTION_CLASS = 'redis.Redis'
""" The path for the default Redis client class to use.
Defaults to the main `Redis` class within the `redis` module
As imported like `from redis import Redis`
"""


DEFAULT_WORKER_TTL = 420
""" The default Time To Live (TTL) for the Worker in seconds
Defines the effective timeout period for a worker
"""


DEFAULT_JOB_MONITORING_INTERVAL = 30
""" The interval in seconds for Job monitoring
"""


DEFAULT_RESULT_TTL = 500
""" The Time To Live (TTL) in seconds to keep job results
Means that the results will be expired from Redis
after `DEFAULT_RESULT_TTL` seconds
"""


DEFAULT_FAILURE_TTL = 31536000
""" The Time To Live (TTL) in seconds to keep job failure information
Means that the failure information will be expired from Redis
after `DEFAULT_FAILURE_TTL` seconds.
Defaults to 1 YEAR in seconds
"""


DEFAULT_CRON_JOB_HISTORY_LIMIT = 1000
""" The maximum number of spawned job IDs to keep in a cron job's history
"""


DEFAULT_CRON_JOB_HISTORY_TTL = 31536000
""" The Time To Live (TTL) in seconds to keep a cron job's history of spawned jobs.
Refreshed on every write, so history expires if the cron job spawns no jobs for 1 YEAR
"""


DEFAULT_CRON_SCHEDULER_TTL = 120
""" The Time To Live (TTL) in seconds for a CronScheduler's Redis hash.
Refreshed on every heartbeat. Also used as the staleness threshold when
pruning dead schedulers from the scheduler registry.
"""


DEFAULT_SCHEDULER_FALLBACK_PERIOD = 120
""" The amount in seconds it will take for a new scheduler
to pickup tasks after a scheduler has died.
This is used as a safety net to avoid race conditions and duplicates
when using multiple schedulers
"""


DEFAULT_MAINTENANCE_TASK_INTERVAL = 10 * 60
""" The interval to run maintenance tasks
in seconds. Defaults to 10 minutes.
"""


DEFAULT_WEBHOOK_RETRY_INITIAL_DELAY = 30
""" The delay in seconds before a failed webhook delivery is first retried.
Retries then use exponential backoff
(30, 60, 120, ... up to DEFAULT_WEBHOOK_RETRY_MAX_DELAY).
"""


DEFAULT_WEBHOOK_RETRY_MAX_DELAY = 60 * 60
""" The cap on webhook delivery retry backoff, in seconds (1 hour).
"""


DEFAULT_WEBHOOK_DELIVERY_MAX_AGE = 3 * 24 * 60 * 60
""" How long a pending webhook delivery keeps being retried before it is
marked as dead, in seconds. Defaults to 3 days.
"""


DEFAULT_WEBHOOK_DELIVERED_TTL = 24 * 60 * 60
""" How long delivered and dead webhook delivery records are kept in Redis
for audit and deduplication, in seconds. Defaults to 1 day.
"""


DEFAULT_WEBHOOK_RECONCILE_INTERVAL = 60
""" How often workers scan for due webhook deliveries to retry, in seconds.
"""


DEFAULT_WEBHOOK_RECONCILE_BATCH_SIZE = 100
""" Maximum number of due webhook deliveries a single reconciliation pass sends.
"""


DEFAULT_WEBHOOK_RECONCILE_LOCK_TTL = 5 * 60
""" TTL of the global webhook reconciliation lock, in seconds.
"""


CALLBACK_TIMEOUT = 60
""" The timeout period in seconds for Callback functions
Means that Functions used in `success_callback`, `stopped_callback`,
and `failure_callback` will timeout after N seconds
"""


DEFAULT_LOGGING_DATE_FORMAT = '%H:%M:%S'
""" The Date Format to use for RQ logging.
Defaults to Hour:Minute:Seconds on 24hour format
eg.: `15:45:23`
"""


DEFAULT_LOGGING_FORMAT = '%(asctime)s %(message)s'
""" The default Logging Format to use
Uses Python's default attributes as defined
https://docs.python.org/3/library/logging.html#logrecord-attributes
"""


DEFAULT_DEATH_PENALTY_CLASS = 'rq.timeouts.UnixSignalDeathPenalty'
""" The path for the default Death Penalty class to use.
Defaults to the `UnixSignalDeathPenalty` class within the `rq.timeouts` module
"""


UNSERIALIZABLE_RETURN_VALUE_PAYLOAD = 'Unserializable return value'
""" The value that we store in the job's _result property or in the Result's return_value
in case the return value of the actual job is not serializable
"""
