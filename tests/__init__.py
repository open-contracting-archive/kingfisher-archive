from os import getenv

from dotenv import load_dotenv

from ocdskingfisherarchive.archive import Archive


def default_archive():
    load_dotenv()

    return Archive(
        getenv('KINGFISHER_ARCHIVE_BUCKET_NAME'),
        getenv('KINGFISHER_ARCHIVE_DATA_DIRECTORY'),
        getenv('KINGFISHER_ARCHIVE_LOGS_DIRECTORY'),
        'db.sqlite3',
        getenv('KINGFISHER_ARCHIVE_DATABASE_URL'),
    )
