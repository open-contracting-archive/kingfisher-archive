import json
import logging
import logging.config
import os

import click
import pidfile
import sentry_sdk

from ocdskingfisherarchive.archive import Archive
from ocdskingfisherarchive.config import Config
from ocdskingfisherarchive.database_archive import DataBaseArchive
from ocdskingfisherarchive.database_process import DataBaseProcess
from ocdskingfisherarchive.s3 import S3

config = Config()
config.load_user_config()


@click.group()
def cli():
    logging_config_file_full_path = os.path.expanduser('~/.config/ocdskingfisher-archive/logging.json')
    if os.path.isfile(logging_config_file_full_path):
        with open(logging_config_file_full_path) as f:
            logging.config.dictConfig(json.load(f))


@cli.command()
@click.option('-n', '--dry-run', is_flag=True)
def archive(dry_run):
    try:
        with pidfile.PIDFile():
            database_archive = DataBaseArchive(config)
            database_process = DataBaseProcess(config)
            s3 = S3(config)
            archive_worker = Archive(config, database_archive, database_process, s3)
            archive_worker.process(dry_run)
    except pidfile.AlreadyRunningError:
        print('Already running.')


if __name__ == '__main__':
    if config.sentry_dsn:
        sentry_sdk.init(
            dsn=config.sentry_dsn,
        )
    cli()
