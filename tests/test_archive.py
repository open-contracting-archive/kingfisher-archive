import json
import os

import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber
from xxhash import xxh3_128

import ocdskingfisherarchive.s3
from ocdskingfisherarchive.crawl import Crawl
from tests import assert_log, create_crawl_directory, path

with open(path('data.json'), 'rb') as f:
    checksum = xxh3_128(f.read()).hexdigest()
size = 239


def crawl(tmpdir):
    return Crawl('scotland', '20200902_052458', tmpdir.join('data'), tmpdir.join('logs', 'kingfisher'))


@pytest.mark.parametrize('data_files,log_file,exact_cache,latest_cache,expected_return_value,expected_log_message', [
    # No remote directory.
    (None, 'log1.log',
     None, None,
     False, 'skip (no_data_directory) scotland/20200902_052458'),
    ([], 'log1.log',
     None, None,
     False, 'skip (no_data_files) scotland/20200902_052458'),
    (['data.json'], None,
     None, None,
     False, 'skip (no_log_file) scotland/20200902_052458'),
    (['data.json'], 'log_in_progress1.log',
     None, None,
     False, 'skip (not_finished) scotland/20200902_052458'),
    (['data.json'], 'log_sample1.log',
     None, None,
     False, 'skip (not_complete) scotland/20200902_052458'),
    (['data.json'], 'log_not_clean_enough.log',
     None, None,
     False, 'skip (not_clean_enough) scotland/20200902_052458'),
    (['data.json'], 'log_error1.log',
     None, None,
     True, 'ARCHIVE (new_period) scotland/20200902_052458'),

    # Same remote directory.
    # Identical
    (['data.json'], 'log_error1.log',
     {'checksum': checksum, 'bytes': size, 'errors_count': 1, 'files_count': 2}, None,
     False, 'skip (same_period) scotland/20200902_052458'),
    # Same bytes
    (['data.json'], 'log_error1.log',
     {'checksum': 'other', 'bytes': size, 'errors_count': 2, 'files_count': 1}, None,
     False, 'skip (same_period) scotland/20200902_052458'),
    # More bytes, but not 50% more bytes
    (['data.json'], 'log_error1.log',
     {'checksum': 'other', 'bytes': size - 1, 'errors_count': 1, 'files_count': 2}, None,
     False, 'skip (same_period) scotland/20200902_052458'),
    # More bytes, but not 50% more files
    (['data.json'], 'log_error1.log',
     {'checksum': 'other', 'bytes': size - 1, 'errors_count': 1, 'files_count': 1.5}, None,
     False, 'skip (same_period) scotland/20200902_052458'),
    # More bytes, but less clean
    (['data.json'], 'log_error1.log',
     {'checksum': 'other', 'bytes': size - 1, 'errors_count': 0, 'files_count': 2}, None,
     False, 'skip (same_period) scotland/20200902_052458'),
    # More bytes, and 50% more bytes
    (['data.json'], 'log_error1.log',
     {'checksum': 'other', 'bytes': int(size // 1.5), 'errors_count': 1, 'files_count': 2}, None,
     True, 'ARCHIVE (same_period_more_bytes) scotland/20200902_052458'),
    # More bytes, and 50% more files
    (['data.json'], 'log_error1.log',
     {'checksum': 'other', 'bytes': size - 1, 'errors_count': 1, 'files_count': 1}, None,
     True, 'ARCHIVE (same_period_more_files) scotland/20200902_052458'),
    # More bytes, and more clean
    (['data.json'], 'log_error1.log',
     {'checksum': 'other', 'bytes': size - 1, 'errors_count': 2, 'files_count': 2}, None,
     True, 'ARCHIVE (same_period_more_clean) scotland/20200902_052458'),

    # Earlier remote directory.
    # Identical
    (['data.json'], 'log_error1.log',
     None, {'checksum': checksum, 'bytes': size, 'errors_count': 1, 'files_count': 2},
     False, 'skip (2020_1_not_distinct) scotland/20200902_052458'),
    # Same errors
    (['data.json'], 'log_error1.log',
     None, {'checksum': 'other', 'bytes': size, 'errors_count': 1, 'files_count': 2},
     True, 'ARCHIVE (new_period) scotland/20200902_052458'),
    # More errors, fewer files, same bytes
    (['data.json'], 'log_error1.log',
     None, {'checksum': 'other', 'bytes': size, 'errors_count': 0, 'files_count': 3},
     False, 'skip (2020_1_not_distinct_maybe) scotland/20200902_052458'),
    # More errors, same files, fewer bytes
    (['data.json'], 'log_error1.log',
     None, {'checksum': 'other', 'bytes': size + 1, 'errors_count': 0, 'files_count': 2},
     False, 'skip (2020_1_not_distinct_maybe) scotland/20200902_052458'),
])
def test_should_archive(data_files, log_file, exact_cache, latest_cache, expected_return_value, expected_log_message,
                        archive, tmpdir, caplog, monkeypatch):
    if exact_cache:
        metadata = tmpdir.join('metadata.json')
        metadata.write(json.dumps({**{'source_id': 'scotland', 'data_version': '20200930_000000'}, **exact_cache}))
        monkeypatch.setattr(ocdskingfisherarchive.s3.S3, 'get_file', lambda *args: metadata)
    else:
        monkeypatch.setattr(ocdskingfisherarchive.s3.S3, 'load_exact', lambda *args: None)

    if latest_cache:
        load_latest = (Crawl('scotland', '20200101_000000', cache=latest_cache), 2020, 1)
    else:
        load_latest = (None, None, None)
    monkeypatch.setattr(ocdskingfisherarchive.s3.S3, 'load_latest', lambda *args: load_latest)

    create_crawl_directory(tmpdir, data_files, log_file)

    actual_return_value = archive.process_crawl(crawl(tmpdir), dry_run=True)

    assert_log(caplog, 'INFO', expected_log_message)
    assert actual_return_value is expected_return_value


def test_process_crawl(archive, tmpdir, caplog, monkeypatch):
    def download_fileobj(*args, **kwargs):
        raise ClientError(error_response={'Error': {'Code': '404'}}, operation_name='')

    def list_objects_v2(*args, **kwargs):
        return {'KeyCount': 0}

    create_crawl_directory(tmpdir, ['data.json'], 'log_error1.log')
    os.utime(tmpdir.join('data', 'scotland', '20200902_052458'), (1, 1))

    stubber = Stubber(ocdskingfisherarchive.s3.client)
    monkeypatch.setattr(ocdskingfisherarchive.s3, 'client', stubber)
    # See https://github.com/boto/botocore/issues/974
    for method in ('upload_file', 'copy', 'delete_object'):
        monkeypatch.setattr(stubber, method, lambda *args, **kwargs: None, raising=False)
    monkeypatch.setattr(stubber, 'download_fileobj', download_fileobj, raising=False)
    monkeypatch.setattr(stubber, 'list_objects_v2', list_objects_v2, raising=False)
    stubber.activate()

    archive.process()

    stubber.assert_no_pending_responses()

    directories = set()
    filenames = set()
    for root, dirs, files in os.walk(tmpdir):
        root_directory = root[len(str(tmpdir)) + 1:]
        for filename in files:
            filenames.add(os.path.join(root_directory, filename))
        for directory in dirs:
            directories.add(os.path.join(root_directory, directory))

    assert filenames == {'cache.sqlite3'}
    assert directories == {'data', os.path.join('data', 'scotland'), 'logs', os.path.join('logs', 'kingfisher'),
                           os.path.join('logs', 'kingfisher', 'scotland')}
