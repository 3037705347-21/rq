import logging
import logging.config
import sys

import click

from rq.cli.cli import main
from rq.cli.helpers import (
    pass_cli_config,
    read_config_file,
    # setup_loghandlers_from_args is not used when only --logging-level is present
)
from rq.cron import CronScheduler
from rq.cron_plan import DEFAULT_PLAN_ID, CronPlan, reconcile
from rq.exceptions import InvalidCronPlanError


@main.command()
@click.option(
    '--logging-level',
    '-l',
    type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], case_sensitive=False),
    default='INFO',
    show_default=True,  # Explicitly show the default in help text
    help='Set logging level.',
)
@click.option(
    '--plan-id',
    default=None,
    show_default=True,
    help=(
        'ID of a reconciled cron plan to run. The scheduler continuously loads that plan’s '
        'managed jobs from Redis; can be combined with CONFIG_PATH.'
    ),
)
@click.argument('config_path', required=False)
@pass_cli_config
def cron(
    cli_config,
    logging_level,
    # verbose and quiet parameters are removed
    plan_id,
    config_path,
    **options,
):
    """Starts the RQ cron scheduler.

    Requires a configuration file/module defining cron jobs (CONFIG_PATH) and/or
    a reconciled cron plan id (--plan-id). Logging level is controlled by the
    --logging-level option.
    """
    if not config_path and not plan_id:
        raise click.UsageError('Provide a CONFIG_PATH and/or --plan-id to start a cron scheduler.')

    settings = read_config_file(cli_config.config) if cli_config.config else {}
    dict_config = settings.get('DICT_CONFIG')

    # Apply custom logging configuration if provided
    if dict_config:
        logging.config.dictConfig(dict_config)
        logging.getLogger('rq.cron').info('Logging configured via DICT_CONFIG setting.')

    try:
        cron = CronScheduler(connection=cli_config.connection, logging_level=logging_level, plan_id=plan_id)
        if config_path:
            cron.load_config_from_file(config_path)
        cron.start()
    except KeyboardInterrupt:
        click.echo('\nShutting down cron scheduler...')
        sys.exit(0)


@main.group(name='cron-plan')
def cron_plan():
    """Manage declarative cron plans (reviewable manifests reconciled at release time)."""


@cron_plan.command(name='apply')
@click.option(
    '--plan-id',
    default=DEFAULT_PLAN_ID,
    show_default=True,
    help='ID of the plan namespace to reconcile; other namespaces are never touched.',
)
@click.argument('plan_path')
@pass_cli_config
def cron_plan_apply(cli_config, plan_id, plan_path, **options):
    """Reconcile a declarative cron plan against the live managed cron jobs.

    Adds new entries, updates changed ones and removes entries deleted from the
    plan. Jobs outside the plan (other plan IDs or ad-hoc jobs registered
    directly) are left untouched. An invalid plan is rejected as a whole and no
    changes are made.
    """
    try:
        plan = CronPlan.load(plan_path)
        result = reconcile(plan, connection=cli_config.connection, plan_id=plan_id)
    except InvalidCronPlanError as exc:
        click.echo(
            f"Rejected cron plan '{plan_path}' with {len(exc.errors)} error(s); no changes were made:",
            err=True,
        )
        for error in exc.errors:
            click.echo(f'  - {error}', err=True)
        sys.exit(1)

    click.echo(result.summary())
