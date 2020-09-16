import datetime

from ocdskingfisherarchive.archive import Archive
from ocdskingfisherarchive.collection import Collection
from ocdskingfisherarchive.config import Config


def test_no_previous():
    config = Config()
    database_archive = None
    database_process = None
    s3 = None

    archive = Archive(config, database_archive, database_process, s3)
    archive._get_exact_archived_collection = lambda c: None
    archive._get_last_archived_collection = lambda c: None

    collection = Collection(config, 1, 'scotland', datetime.datetime(2020, 9, 2, 5, 25, 00))

    assert True == archive.should_we_archive_collection(collection)  # noqa: E712
