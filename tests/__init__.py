import datetime
import os.path
from os import getenv

from ocdskingfisherarchive.archive import Archive
from ocdskingfisherarchive.collection import Collection
from ocdskingfisherarchive.scrapy_log_file import ScrapyLogFile


def log_file_path(filename):
    return os.path.join('tests', 'logs', filename)


def archive_fixture():
    archive = Archive(
        getenv('KINGFISHER_ARCHIVE_BUCKET_NAME'),
        os.path.join('tests', 'data'),
        os.path.join('tests', 'logs'),
        'db.sqlite3',
        getenv('KINGFISHER_ARCHIVE_DATABASE_URL'),
    )

    return archive


def collection_fixture(*, md5='eo39tj38jm', size=186306, data_exists=True, errors_count=0, spider_arguments=None,
                       is_finished=True):
    if spider_arguments is None:
        spider_arguments = {}

    collection = Collection(1, 'scotland', datetime.datetime(2020, 9, 2, 5, 25, 0))
    collection._data_md5 = md5
    collection._data_size = size
    collection.get_data_files_exist = lambda: data_exists
    collection.scrapy_log_file = ScrapyLogFile('test.log')
    collection.scrapy_log_file._errors_sent_to_process_count = errors_count
    collection.scrapy_log_file._spider_arguments = spider_arguments
    collection.scrapy_log_file.is_finished = lambda: is_finished

    return collection
