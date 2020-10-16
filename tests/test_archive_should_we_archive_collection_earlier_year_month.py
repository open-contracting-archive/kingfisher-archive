import datetime

from ocdskingfisherarchive.archive import Archive
from ocdskingfisherarchive.archived_collection import ArchivedCollection
from ocdskingfisherarchive.collection import Collection
from ocdskingfisherarchive.scrapy_log_file import ScrapyLogFile


def test_backup():
    """" This source was archived before this month. We should archive. """
    archive = Archive(None, None, None)
    archive._get_exact_archived_collection = lambda c: None
    archive._get_last_archived_collection = \
        lambda c: ArchivedCollection({'data_md5': 'oeu7394ud48h', 'data_size': 123456}, 2020, 9)

    collection = Collection(1, 'scotland', datetime.datetime(2020, 9, 2, 5, 25, 00))
    collection.get_md5_of_data_folder = lambda: 'eo39tj38jm'
    collection.get_size_of_data_folder = lambda: 386306
    collection.get_data_files_exist = lambda: True
    collection._cached_scrapy_log_file_name = 'test.log'
    collection._cached_scrapy_log_file = ScrapyLogFile('test.log')
    collection._cached_scrapy_log_file._errors_sent_to_process_count = 0
    collection._cached_scrapy_log_file._spider_arguments = {}
    collection._cached_scrapy_log_file.is_finished = lambda: True

    assert archive.should_we_archive_collection(collection) is True


def test_backup_zero_errors_slightly_bigger_size():
    """" This source was archived before this month.
    Both collections have zero errors, but local is slightly bigger (not 50% bigger)
    We should archive. """
    archive = Archive(None, None, None)
    archive._get_exact_archived_collection = lambda c: None
    archive._get_last_archived_collection = \
        lambda c: ArchivedCollection(
            {'data_md5': 'oeu7394ud48h', 'data_size': 123456, 'errors_count': 0, 'scrapy_log_file_found': True},
            2020,
            9
        )

    collection = Collection(1, 'scotland', datetime.datetime(2020, 9, 2, 5, 25, 00))
    collection.get_md5_of_data_folder = lambda: 'eo39tj38jm'
    collection.get_size_of_data_folder = lambda: 123457
    collection.get_data_files_exist = lambda: True
    collection._cached_scrapy_log_file_name = 'test.log'
    collection._cached_scrapy_log_file = ScrapyLogFile('test.log')
    collection._cached_scrapy_log_file._errors_sent_to_process_count = 0
    collection._cached_scrapy_log_file._spider_arguments = {}
    collection._cached_scrapy_log_file.is_finished = lambda: True

    assert archive.should_we_archive_collection(collection) is True


def test_same_md5():
    """" This source was archived before this month.  MD5 is the same so don't back up.  """
    archive = Archive(None, None, None)
    archive._get_exact_archived_collection = lambda c: None
    archive._get_last_archived_collection = \
        lambda c: ArchivedCollection({'data_md5': 'eo39tj38jm', 'data_size': 123456}, 2020, 1)

    collection = Collection(1, 'scotland', datetime.datetime(2020, 9, 2, 5, 25, 00))
    collection.get_md5_of_data_folder = lambda: 'eo39tj38jm'
    collection.get_size_of_data_folder = lambda: 386306
    collection.get_data_files_exist = lambda: True
    collection._cached_scrapy_log_file_name = 'test.log'
    collection._cached_scrapy_log_file = ScrapyLogFile('test.log')
    collection._cached_scrapy_log_file._errors_sent_to_process_count = 0
    collection._cached_scrapy_log_file._spider_arguments = {}
    collection._cached_scrapy_log_file.is_finished = lambda: True

    assert archive.should_we_archive_collection(collection) is False


def test_size_not_50_percent_more():
    """" This source was archived before this month.  The size is slightly larger but not 50% so don't back up."""
    archive = Archive(None, None, None)
    archive._get_exact_archived_collection = lambda c: None
    archive._get_last_archived_collection = \
        lambda c: ArchivedCollection({'data_md5': 'oeu7394ud48h', 'data_size': 123456}, 2020, 1)

    collection = Collection(1, 'scotland', datetime.datetime(2020, 9, 2, 5, 25, 00))
    collection.get_md5_of_data_folder = lambda: 'eo39tj38jm'
    collection.get_size_of_data_folder = lambda: 133456
    collection.get_data_files_exist = lambda: True
    collection._cached_scrapy_log_file_name = 'test.log'
    collection._cached_scrapy_log_file = ScrapyLogFile('test.log')
    collection._cached_scrapy_log_file._errors_sent_to_process_count = 0
    collection._cached_scrapy_log_file._spider_arguments = {}
    collection._cached_scrapy_log_file.is_finished = lambda: True

    assert archive.should_we_archive_collection(collection) is False


def test_less_errors():
    """" This source was archived before this month.  Local Collection has less errors, so backup.  """
    archive = Archive(None, None, None)
    archive._get_exact_archived_collection = lambda c: None
    archive._get_last_archived_collection = \
        lambda c: ArchivedCollection(
            {'data_md5': 'oeu7394ud48h', 'data_size': 123456, 'scrapy_log_file_found': True, 'errors_count': 1},
            2020,
            9
        )
    collection = Collection(1, 'scotland', datetime.datetime(2020, 9, 2, 5, 25, 00))
    collection.get_md5_of_data_folder = lambda: 'eo39tj38jm'
    collection.get_size_of_data_folder = lambda: 123456
    collection.get_data_files_exist = lambda: True
    collection._cached_scrapy_log_file_name = 'test.log'
    collection._cached_scrapy_log_file = ScrapyLogFile('test.log')
    collection._cached_scrapy_log_file._errors_sent_to_process_count = 0
    collection._cached_scrapy_log_file._spider_arguments = {}
    collection._cached_scrapy_log_file.is_finished = lambda: True

    assert archive.should_we_archive_collection(collection) is True


def test_less_errors_but_smaller_size():
    """This source was archived before this month.
    Local Collection has less errors but is smaller size, so don't backup."""
    archive = Archive(None, None, None)
    archive._get_exact_archived_collection = lambda c: None
    archive._get_last_archived_collection = \
        lambda c: ArchivedCollection(
            {'data_md5': 'oeu7394ud48h', 'data_size': 123456, 'scrapy_log_file_found': True, 'errors_count': 1},
            2020,
            9
        )
    collection = Collection(1, 'scotland', datetime.datetime(2020, 9, 2, 5, 25, 00))
    collection.get_md5_of_data_folder = lambda: 'eo39tj38jm'
    collection.get_size_of_data_folder = lambda: 100456
    collection.get_data_files_exist = lambda: True
    collection._cached_scrapy_log_file_name = 'test.log'
    collection._cached_scrapy_log_file = ScrapyLogFile('test.log')
    collection._cached_scrapy_log_file._errors_sent_to_process_count = 0
    collection._cached_scrapy_log_file._spider_arguments = {}
    collection._cached_scrapy_log_file.is_finished = lambda: True

    assert archive.should_we_archive_collection(collection) is False
