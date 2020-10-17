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

load_dotenv()


@click.group()
def cli():
    logging_config_file_full_path = os.path.expanduser('~/.config/ocdskingfisher-archive/logging.json')
    if os.path.isfile(logging_config_file_full_path):
        with open(logging_config_file_full_path) as f:
            logging.config.dictConfig(json.load(f))


@cli.command()
@click.option('-b', '--bucket-name',
              help='The Amazon S3 bucket name')
@click.option('--data-directory',
              help="Kingfisher Collect's FILES_STORE directory")
@click.option('--logs-directory',
              help="Kingfisher Collect's project directory within Scrapyd's logs_dir directory")
@click.option('--database-file', default='db.sqlite3',
              help='The SQLite database for caching the local state (defaults to db.sqlite3)')
@click.option('--database-url',
              help="Kingfisher Process' database URL")
@click.option('-n', '--dry-run', is_flag=True,
              help="Don't archive any files, just show whether they would be")
def archive(bucket_name, data_directory, logs_directory, database_file, database_url, dry_run):
    """
    Archives data and log files written by Kingfisher Collect to Amazon S3.
    """
    try:
        with pidfile.PIDFile():
            Archive(database_file, bucket_name, database_url, data_directory, logs_directory).process(dry_run)
    except pidfile.AlreadyRunningError:
        print('Already running.')


if __name__ == '__main__':
    if 'SENTRY_DSN' in os.environ:
        sentry_sdk.init(dsn=os.getenv('SENTRY_DSN'))
    cli(auto_envvar_prefix='KINGFISHER_ARCHIVE')
