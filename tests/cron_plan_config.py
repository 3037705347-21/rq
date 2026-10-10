from rq.cron_plan import define
from tests.fixtures import div_by_zero, do_nothing, say_hello

# 1. Basic interval job with an explicit name
define(say_hello, queue_name='default', name='plan-hello', interval=60)

# 2. Cron based job with arguments
define(
    do_nothing,
    queue_name='default',
    cron='*/5 * * * *',
)

# 3. Interval job with args (fails at runtime, used to assert args are persisted)
define(
    div_by_zero,
    queue_name='default',
    args=(10,),
    interval=300,
    name='plan-failing',
)
