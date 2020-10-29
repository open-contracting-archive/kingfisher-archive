#!/usr/bin/env python
import logging
import logging.config
import os

import click
import pidfile
import sentry_sdk
from dotenv import load_dotenv

from ocdskingfisherarchive.archive import Archive


@click.group()
def cli():
    load_dotenv()


@cli.command()
@click.option('-b', '--bucket-name', envvar='KINGFISHER_ARCHIVE_BUCKET_NAME',
              help='The Amazon S3 bucket name')
@click.option('--data-directory', envvar='KINGFISHER_ARCHIVE_DATA_DIRECTORY',
              help="Kingfisher Collect's FILES_STORE directory")
@click.option('--logs-directory', envvar='KINGFISHER_ARCHIVE_LOGS_DIRECTORY',
              help="Kingfisher Collect's project directory within Scrapyd's logs_dir directory")
@click.option('--cache-file', default='cache.sqlite3', envvar='KINGFISHER_ARCHIVE_CACHE_FILE',
              help='The SQLite database for caching the local state (defaults to cache.sqlite3)')
@click.option('--logging-config-file', envvar='KINGFISHER_ARCHIVE_LOGGING_CONFIG_FILE',
              help="A JSON file following Python's logging configuration dictionary schema")
@click.option('-n', '--dry-run', is_flag=True,
              help="Don't archive any files, just show whether they would be")
@click.option('--invalidate-cache', is_flag=True,
              help="Ignore and overwrite existing rows in the SQLite database")
def archive(bucket_name, data_directory, logs_directory, cache_file, logging_config_file, dry_run):
    """
    Archives data and log files written by Kingfisher Collect to Amazon S3.
    """
    if logging_config_file:
        logging.config.fileConfig(logging_config_file)
    else:
        logging.basicConfig(level=logging.INFO)

    if not bucket_name:
        raise click.UsageError('--bucket-name or KINGFISHER_ARCHIVE_BUCKET_NAME must be set')
    if not data_directory:
        raise click.UsageError('--data-directory or KINGFISHER_ARCHIVE_DATA_DIRECTORY must be set')
    if not logs_directory:
        raise click.UsageError('--logs-directory or KINGFISHER_ARCHIVE_LOGS_DIRECTORY must be set')

    # We don't catch pidfile.AlreadyRunningError so that it can be raised to Sentry. If this error is raised by a cron
    # job, it points to either a very slow archival process, or to an unanticipated problem.
    with pidfile.PIDFile():
        Archive(bucket_name, data_directory, logs_directory, cache_file, invalidate_cache).process(dry_run)


if __name__ == '__main__':
    if 'SENTRY_DSN' in os.environ:
        sentry_sdk.init(dsn=os.getenv('SENTRY_DSN'))
    cli()
