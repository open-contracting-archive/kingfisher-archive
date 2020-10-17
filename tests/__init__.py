import os.path
from os import getenv

from dotenv import load_dotenv

from ocdskingfisherarchive.archive import Archive


def path(filename):
    return os.path.join('tests', 'logs', filename)


def default_archive():
    load_dotenv()

    return Archive(
        getenv('KINGFISHER_ARCHIVE_BUCKET_NAME'),
        os.path.join('tests', 'data'),
        os.path.join('tests', 'logs'),
        'db.sqlite3',
        getenv('KINGFISHER_ARCHIVE_DATABASE_URL'),
    )
