import click
import pidfile

from ocdskingfisherarchive.archive import Archive
from ocdskingfisherarchive.config import Config

config = Config()
config.load_user_config()


@click.group()
def cli():
    pass


@cli.command()
def archive():
    try:
        with pidfile.PIDFile():
            archive = Archive(config)
            archive.archive()
    except pidfile.AlreadyRunningError:
        print('Already running.')


if __name__ == '__main__':
    cli()
