import datetime

from ocdskingfisherarchive.archived_collection import ArchivedCollection
from ocdskingfisherarchive.collection import Collection
from ocdskingfisherarchive.scrapy_log_file import ScrapyLogFile
from tests import default_archive


def test_backup():
    """This source was archived this year/month. Local Collection is bigger and with a different MD5 so backup. """
    archive = default_archive()
    archive._get_exact_archived_collection = \
        lambda c: ArchivedCollection({'data_md5': 'oeu7394ud48h', 'data_size': 123456}, 2020, 9)
    archive._get_last_archived_collection = lambda c: None

    collection = Collection(1, 'scotland', datetime.datetime(2020, 9, 2, 5, 25, 00))
    collection.get_md5_of_data_folder = lambda: 'eo39tj38jm'
    collection.get_size_of_data_folder = lambda: 186306
    collection.get_data_files_exist = lambda: True
    collection._cached_scrapy_log_file_name = 'test.log'
    collection._cached_scrapy_log_file = ScrapyLogFile('test.log')
    collection._cached_scrapy_log_file._errors_sent_to_process_count = 0
    collection._cached_scrapy_log_file._spider_arguments = {}
    collection._cached_scrapy_log_file.is_finished = lambda: True

    assert archive.should_we_archive_collection(collection) is True


def test_same_md5():
    """This source was archived this year/month. MD5 is the same so don't back up. """
    archive = default_archive()
    archive._get_exact_archived_collection = \
        lambda c: ArchivedCollection({'data_md5': 'eo39tj38jm', 'data_size': 123456}, 2020, 9)
    archive._get_last_archived_collection = lambda c: None

    collection = Collection(1, 'scotland', datetime.datetime(2020, 9, 2, 5, 25, 00))
    collection.get_md5_of_data_folder = lambda: 'eo39tj38jm'
    collection.get_size_of_data_folder = lambda: 186306
    collection.get_data_files_exist = lambda: True
    collection._cached_scrapy_log_file_name = 'test.log'
    collection._cached_scrapy_log_file = ScrapyLogFile('test.log')
    collection._cached_scrapy_log_file._errors_sent_to_process_count = 0
    collection._cached_scrapy_log_file._spider_arguments = {}
    collection._cached_scrapy_log_file.is_finished = lambda: True

    assert archive.should_we_archive_collection(collection) is False


def test_smaller_size():
    """This source was archived this year/month. Local Collection is smaller, so don't backup. """
    archive = default_archive()
    archive._get_exact_archived_collection = \
        lambda c: ArchivedCollection({'data_md5': 'oeu7394ud48h', 'data_size': 234813}, 2020, 9)
    archive._get_last_archived_collection = lambda c: None

    collection = Collection(1, 'scotland', datetime.datetime(2020, 9, 2, 5, 25, 00))
    collection.get_md5_of_data_folder = lambda: 'eo39tj38jm'
    collection.get_size_of_data_folder = lambda: 186306
    collection.get_data_files_exist = lambda: True
    collection._cached_scrapy_log_file_name = 'test.log'
    collection._cached_scrapy_log_file = ScrapyLogFile('test.log')
    collection._cached_scrapy_log_file._errors_sent_to_process_count = 0
    collection._cached_scrapy_log_file._spider_arguments = {}
    collection._cached_scrapy_log_file.is_finished = lambda: True

    assert archive.should_we_archive_collection(collection) is False


def test_more_errors():
    """This source was archived this year/month. Local Collection has more errors, so don't backup. """
    archive = default_archive()
    archive._get_exact_archived_collection = \
        lambda c: ArchivedCollection(
            {'data_md5': 'oeu7394ud48h', 'data_size': 123456, 'scrapy_log_file_found': True, 'errors_count': 0},
            2020, 9
        )
    archive._get_last_archived_collection = lambda c: None

    collection = Collection(1, 'scotland', datetime.datetime(2020, 9, 2, 5, 25, 00))
    collection.get_md5_of_data_folder = lambda: 'eo39tj38jm'
    collection.get_size_of_data_folder = lambda: 186306
    collection.get_data_files_exist = lambda: True
    collection._cached_scrapy_log_file_name = 'test.log'
    collection._cached_scrapy_log_file = ScrapyLogFile('test.log')
    collection._cached_scrapy_log_file._errors_sent_to_process_count = 1
    collection._cached_scrapy_log_file._spider_arguments = {}
    collection._cached_scrapy_log_file.is_finished = lambda: True

    assert archive.should_we_archive_collection(collection) is False
