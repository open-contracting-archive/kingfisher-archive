#!/usr/bin/env python
import json
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
@click.option('-b', '--bucket-name',
              help='The Amazon S3 bucket name')
@click.option('--data-directory',
              help="Kingfisher Collect's FILES_STORE directory")
@click.option('--logs-directory',
              help="Kingfisher Collect's project directory within Scrapyd's logs_dir directory")
@click.option('--database-file', default='db.sqlite3',
              help='The SQLite database for caching the local state (defaults to db.sqlite3)')
@click.option('--logging-config-file',
              help="A JSON file following Python's logging configuration dictionary schema")
@click.option('-n', '--dry-run', is_flag=True,
              help="Don't archive any files, just show whether they would be")
def archive(bucket_name, data_directory, logs_directory, database_file, logging_config_file, dry_run):
    """
    Archives data and log files written by Kingfisher Collect to Amazon S3.
    """
    if logging_config_file:
        with open(logging_config_file) as f:
            logging.config.dictConfig(json.load(f))
    else:
        logging.basicConfig(level=logging.INFO)

    if not bucket_name:
        raise click.UsageError('--bucket-name or KINGFISHER_ARCHIVE_BUCKET_NAME must be set')
    if not data_directory:
        raise click.UsageError('--data-directory or KINGFISHER_ARCHIVE_DATA_DIRECTORY must be set')
    if not logs_directory:
        raise click.UsageError('--logs-directory or KINGFISHER_ARCHIVE_LOGS_DIRECTORY must be set')

    try:
        with pidfile.PIDFile():
            Archive(bucket_name, data_directory, logs_directory, database_file).process(dry_run)
    except pidfile.AlreadyRunningError:
        raise click.UsageError('Already running.')


if __name__ == '__main__':
    if 'SENTRY_DSN' in os.environ:
        sentry_sdk.init(dsn=os.getenv('SENTRY_DSN'))
    cli(auto_envvar_prefix='KINGFISHER')
