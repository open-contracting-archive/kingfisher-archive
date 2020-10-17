import datetime
import os.path
from os import getenv

from dotenv import load_dotenv

from ocdskingfisherarchive.archive import Archive
from ocdskingfisherarchive.archived_collection import ArchivedCollection
from ocdskingfisherarchive.collection import Collection
from ocdskingfisherarchive.scrapy_log_file import ScrapyLogFile

load_dotenv()


def assert_log(caplog, levelname, message):
    assert len(caplog.records) == 1
    assert caplog.records[0].name == 'ocdskingfisher.archive'
    assert caplog.records[0].levelname == levelname, f'{caplog.records[0].levelname!r} == {levelname!r}'
    assert caplog.records[0].message == message, f'{caplog.records[0].message!r} == {message!r}'


def log_file_path(filename):
    return os.path.join('tests', 'logs', filename)


def archive_fixture(*, exact=None, last=None):
    if exact is not None:
        exact = ArchivedCollection(*exact)
    if last is not None:
        last = ArchivedCollection(*last)

    archive = Archive(
        getenv('KINGFISHER_ARCHIVE_BUCKET_NAME'),
        os.path.join('tests', 'data'),
        os.path.join('tests', 'logs'),
        'db.sqlite3',
        getenv('KINGFISHER_ARCHIVE_DATABASE_URL'),
    )
    archive._get_exact_archived_collection = lambda c: exact
    archive._get_last_archived_collection = lambda c: last

    return archive


def collection_fixture(*, md5='eo39tj38jm', size=186306, data_exists=True, errors_count=0, spider_arguments=None,
                       is_finished=True):
    if spider_arguments is None:
        spider_arguments = {}

    collection = Collection(1, 'scotland', datetime.datetime(2020, 9, 2, 5, 25, 0))
    collection._data_md5 = md5
    collection._data_size = size
    collection.get_data_files_exist = lambda: data_exists
    collection._cached_scrapy_log_file = ScrapyLogFile('test.log')
    collection._cached_scrapy_log_file._errors_sent_to_process_count = errors_count
    collection._cached_scrapy_log_file._spider_arguments = spider_arguments
    collection._cached_scrapy_log_file.is_finished = lambda: is_finished

    return collection
