from unittest.mock import patch

import pytest

from ocdskingfisherarchive.s3 import S3
from tests import archive, assert_log, collection, create_crawl_directory

# md5 tests/fixtures/data.json
md5 = '815a9cd4ee14b875834cd019238a8705'
size = 239


@pytest.mark.parametrize('data_files, log_file, load_exact, load_latest, expected_return_value, message_log_message', [
    # No remote directory.
    (['data.json'], 'log_error1.log',
     None, (None, None, None),
     True, 'Archiving 1 because no current or previous archives found'),
    (['data.json'], 'log_sample1.log',
     None, (None, None, None),
     False, 'Skipping 1 because collection is a subset'),
    (None, 'log1.log',
     None, (None, None, None),
     False, 'Skipping 1 because data files do not exist'),
    (['data.json'], 'log_in_progress1.log',
     None, (None, None, None),
     False, 'Skipping 1 because Scrapy log file says it is not finished'),
    (['data.json'], None,
     None, (None, None, None),
     False, 'Skipping 1 because log file does not exist'),

    # Same remote directory.
    (['data.json'], 'log_error1.log',
     {'data_md5': md5, 'data_size': size, 'errors_count': 1}, (None, None, None),
     False, 'Skipping 1 because an archive exists for same period and same MD5'),
    (['data.json'], 'log_error1.log',
     {'data_md5': 'other', 'data_size': 1000000, 'errors_count': 1}, (None, None, None),
     False, 'Skipping 1 because an archive exists for same period and same or larger size'),
    (['data.json'], 'log_error1.log',
     {'data_md5': 'other', 'data_size': size, 'errors_count': 0}, (None, None, None),
     False, 'Skipping 1 because an archive exists for same period and fewer errors'),
    (['data.json'], 'log_error1.log',
     {'data_md5': 'other', 'data_size': size - 1, 'errors_count': 1}, (None, None, None),
     True, 'Archiving 1 because an archive exists for same period and we can not find a good reason to not archive'),

    # Earlier remote directory.
    (['data.json'], 'log_error1.log',
     None, ({'data_md5': md5, 'data_size': size, 'errors_count': 1}, 2020, 1),
     False, 'Skipping 1 because an archive exists from earlier period (2020/1) and same MD5'),
    (['data.json'], 'log_error1.log',
     None, ({'data_md5': 'other', 'data_size': size - 1, 'errors_count': None}, 2020, 1),
     False, 'Skipping 1 because an archive exists from earlier period (2020/1) and we can not find a good reason to '
            'backup'),
    (['data.json'], 'log_error1.log',
     None, ({'data_md5': 'other', 'data_size': size - 1, 'errors_count': 1}, 2020, 9),
     True, 'Archiving 1 because an archive exists from earlier period (2020/9) and local collection has fewer or '
           'equal errors and greater or equal size'),
    (['data.json'], 'log_error1.log',
     None, ({'data_md5': 'other', 'data_size': 1, 'errors_count': 1}, 2020, 9),
     True, 'Archiving 1 because an archive exists from earlier period (2020/9) and local collection has 50% more '
           'size'),
    (['data.json'], 'log_error1.log',
     None, ({'data_md5': 'other', 'data_size': size, 'errors_count': 2}, 2020, 9),
     True, 'Archiving 1 because an archive exists from earlier period (2020/9) and local collection has fewer or '
           'equal errors and greater or equal size'),
    (['data.json'], 'log_error1.log',
     None, ({'data_md5': 'other', 'data_size': size + 1, 'errors_count': 2}, 2020, 9),
     False, 'Skipping 1 because an archive exists from earlier period (2020/9) and we can not find a good reason to '
            'backup'),
])
def test_should_we_archive_collection(data_files, log_file, load_exact, load_latest, expected_return_value,
                                      message_log_message, tmpdir, caplog, monkeypatch):
    monkeypatch.setattr(S3, 'load_exact', lambda *args: load_exact)
    monkeypatch.setattr(S3, 'load_latest', lambda *args: load_latest)
    create_crawl_directory(tmpdir, data_files, log_file)

    actual_return_value = archive(tmpdir).should_we_archive_collection(collection(tmpdir))

    assert_log(caplog, 'INFO', message_log_message)
    assert actual_return_value is expected_return_value
