import datetime

from ocdskingfisherarchive.archive import Archive
from ocdskingfisherarchive.archived_collection import ArchivedCollection
from ocdskingfisherarchive.collection import Collection
from ocdskingfisherarchive.config import Config


def test_backup():
    config = Config()
    database_archive = None
    database_process = None
    s3 = None

    archive = Archive(config, database_archive, database_process, s3)
    archive._get_exact_archived_collection = \
        lambda c: ArchivedCollection({'data_md5': 'oeu7394ud48h', 'data_size': 123456}, 2020, 9)
    archive._get_last_archived_collection = lambda c: None

    collection = Collection(config, 1, 'scotland', datetime.datetime(2020, 9, 2, 5, 25, 00))
    collection.get_md5_of_data_folder = lambda: 'eo39tj38jm'
    collection.get_size_of_data_folder = lambda: 186306

    assert True == archive.should_we_archive_collection(collection)  # noqa: E712


def test_same_md5():
    config = Config()
    database_archive = None
    database_process = None
    s3 = None

    archive = Archive(config, database_archive, database_process, s3)
    archive._get_exact_archived_collection = \
        lambda c: ArchivedCollection({'data_md5': 'eo39tj38jm', 'data_size': 123456}, 2020, 9)
    archive._get_last_archived_collection = lambda c: None

    collection = Collection(config, 1, 'scotland', datetime.datetime(2020, 9, 2, 5, 25, 00))
    collection.get_md5_of_data_folder = lambda: 'eo39tj38jm'
    collection.get_size_of_data_folder = lambda: 186306

    assert False == archive.should_we_archive_collection(collection)  # noqa: E712


def test_smaller_size():
    config = Config()
    database_archive = None
    database_process = None
    s3 = None

    archive = Archive(config, database_archive, database_process, s3)
    archive._get_exact_archived_collection = \
        lambda c: ArchivedCollection({'data_md5': 'oeu7394ud48h', 'data_size': 234813}, 2020, 9)
    archive._get_last_archived_collection = lambda c: None

    collection = Collection(config, 1, 'scotland', datetime.datetime(2020, 9, 2, 5, 25, 00))
    collection.get_md5_of_data_folder = lambda: 'eo39tj38jm'
    collection.get_size_of_data_folder = lambda: 186306

    assert False == archive.should_we_archive_collection(collection)  # noqa: E712
