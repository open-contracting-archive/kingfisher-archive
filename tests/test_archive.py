import datetime
import os

import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

import ocdskingfisherarchive.s3
from tests import path
from ocdskingfisherarchive.archive import Archive
from ocdskingfisherarchive.crawl import Crawl

# md5 tests/fixtures/data.json
md5 = '815a9cd4ee14b875834cd019238a8705'
size = 239


def create_crawl_directory(tmpdir, data, log):
    data_directory = tmpdir.mkdir('data')
    spider_directory = data_directory.mkdir('scotland')

    if data is not None:
        crawl_directory = spider_directory.mkdir('20200902_052458')
        for i, name in enumerate(data):
            file = crawl_directory.join(f'{i}.json')
            with open(path(name)) as f:
                file.write(f.read())

    logs_directory = tmpdir.mkdir('logs')
    project_directory = logs_directory.mkdir('kingfisher')
    spider_directory = project_directory.mkdir('scotland')

    if log:
        file = spider_directory.join('307e8331edc801c691e21690db130256.log')
        with open(path(log)) as f:
            file.write(f.read())


def archive(tmpdir):
    return Archive(
        os.getenv('KINGFISHER_ARCHIVE_BUCKET_NAME'),
        tmpdir.join('data'),
        tmpdir.join('logs', 'kingfisher'),
        'db.sqlite3',
        os.getenv('KINGFISHER_ARCHIVE_DATABASE_URL'),
    )


def crawl(tmpdir):
    return Crawl('scotland', datetime.datetime(2020, 9, 2, 5, 24, 58), tmpdir.join('data'),
                 tmpdir.join('logs', 'kingfisher'))


def assert_log(caplog, levelname, message):
    assert len(caplog.records) == 1
    assert caplog.records[0].name == 'ocdskingfisher.archive'
    assert caplog.records[0].levelname == levelname, f'{caplog.records[0].levelname!r} == {levelname!r}'
    assert caplog.records[0].message == message, f'{caplog.records[0].message!r} == {message!r}'


@pytest.mark.parametrize('data_files, log_file, load_exact, load_latest, expected_return_value, message_log_message', [
    # No remote directory.
    (['data.json'], 'log_error1.log',
     None, (None, None, None),
     True, 'Archiving scotland/20200902_052458 because no current or previous archives found'),
    (['data.json'], 'log_sample1.log',
     None, (None, None, None),
     False, 'Skipping scotland/20200902_052458 because crawl is a subset'),
    (None, 'log1.log',
     None, (None, None, None),
     False, 'Skipping scotland/20200902_052458 because data files do not exist'),
    (['data.json'], 'log_in_progress1.log',
     None, (None, None, None),
     False, 'Skipping scotland/20200902_052458 because Scrapy log file says it is not finished'),
    (['data.json'], None,
     None, (None, None, None),
     False, 'Skipping scotland/20200902_052458 because log file does not exist'),

    # Same remote directory.
    (['data.json'], 'log_error1.log',
     {'data_md5': md5, 'data_size': size, 'errors_count': 1}, (None, None, None),
     False, 'Skipping scotland/20200902_052458 because an archive exists for same period and same MD5'),
    (['data.json'], 'log_error1.log',
     {'data_md5': 'other', 'data_size': 1000000, 'errors_count': 1}, (None, None, None),
     False, 'Skipping scotland/20200902_052458 because an archive exists for same period and same or larger size'),
    (['data.json'], 'log_error1.log',
     {'data_md5': 'other', 'data_size': size, 'errors_count': 0}, (None, None, None),
     False, 'Skipping scotland/20200902_052458 because an archive exists for same period and fewer errors'),
    (['data.json'], 'log_error1.log',
     {'data_md5': 'other', 'data_size': size - 1, 'errors_count': 1}, (None, None, None),
     True, 'Archiving scotland/20200902_052458 because an archive exists for same period and we can not find a good '
           'reason to not archive'),

    # Earlier remote directory.
    (['data.json'], 'log_error1.log',
     None, ({'data_md5': md5, 'data_size': size, 'errors_count': 1}, 2020, 1),
     False, 'Skipping scotland/20200902_052458 because an archive exists from earlier period (2020/1) and same MD5'),
    (['data.json'], 'log_error1.log',
     None, ({'data_md5': 'other', 'data_size': size - 1, 'errors_count': None}, 2020, 1),
     False, 'Skipping scotland/20200902_052458 because an archive exists from earlier period (2020/1) and we can not '
            'find a good reason to backup'),
    (['data.json'], 'log_error1.log',
     None, ({'data_md5': 'other', 'data_size': size - 1, 'errors_count': 1}, 2020, 9),
     True, 'Archiving scotland/20200902_052458 because an archive exists from earlier period (2020/9) and local '
           'crawl has fewer or equal errors and greater or equal size'),
    (['data.json'], 'log_error1.log',
     None, ({'data_md5': 'other', 'data_size': 1, 'errors_count': 1}, 2020, 9),
     True, 'Archiving scotland/20200902_052458 because an archive exists from earlier period (2020/9) and local '
           'crawl has 50% more size'),
    (['data.json'], 'log_error1.log',
     None, ({'data_md5': 'other', 'data_size': size, 'errors_count': 2}, 2020, 9),
     True, 'Archiving scotland/20200902_052458 because an archive exists from earlier period (2020/9) and local '
           'crawl has fewer or equal errors and greater or equal size'),
    (['data.json'], 'log_error1.log',
     None, ({'data_md5': 'other', 'data_size': size + 1, 'errors_count': 2}, 2020, 9),
     False, 'Skipping scotland/20200902_052458 because an archive exists from earlier period (2020/9) and we can not '
            'find a good reason to backup'),
])
def test_should_we_archive_crawl(data_files, log_file, load_exact, load_latest, expected_return_value,
                                 message_log_message, tmpdir, caplog, monkeypatch):
    monkeypatch.setattr(ocdskingfisherarchive.s3.S3, 'load_exact', lambda *args: load_exact)
    monkeypatch.setattr(ocdskingfisherarchive.s3.S3, 'load_latest', lambda *args: load_latest)
    create_crawl_directory(tmpdir, data_files, log_file)

    actual_return_value = archive(tmpdir).should_we_archive_crawl(crawl(tmpdir))

    assert_log(caplog, 'INFO', message_log_message)
    assert actual_return_value is expected_return_value


def test_process_crawl(tmpdir, caplog, monkeypatch):
    def download_fileobj(*args, **kwargs):
        raise ClientError(error_response={'Error': {'Code': '404'}}, operation_name='')

    def list_objects_v2(*args, **kwargs):
        return {'KeyCount': 0}

    create_crawl_directory(tmpdir, ['data.json'], 'log_error1.log')

    stubber = Stubber(ocdskingfisherarchive.s3.client)
    monkeypatch.setattr(ocdskingfisherarchive.s3, 'client', stubber)
    # See https://github.com/boto/botocore/issues/974
    for method in ('upload_file', 'copy', 'delete_object'):
        monkeypatch.setattr(stubber, method, lambda *args, **kwargs: None, raising=False)
    monkeypatch.setattr(stubber, 'download_fileobj', download_fileobj, raising=False)
    monkeypatch.setattr(stubber, 'list_objects_v2', list_objects_v2, raising=False)
    stubber.activate()

    archive(tmpdir).process_crawl(crawl(tmpdir))

    stubber.assert_no_pending_responses()

    directories = set()
    filenames = set()
    for root, dirs, files in os.walk(tmpdir):
        root_directory = root[len(str(tmpdir)) + 1:]
        for filename in files:
            filenames.add(os.path.join(root_directory, filename))
        for directory in dirs:
            directories.add(os.path.join(root_directory, directory))

    assert not filenames
    assert directories == {'data', os.path.join('data', 'scotland'), 'logs', os.path.join('logs', 'kingfisher'),
                           os.path.join('logs', 'kingfisher', 'scotland')}
