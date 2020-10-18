import datetime

import pytest

from ocdskingfisherarchive.scrapy_log_file import ScrapyLogFile
from tests import log_file_path


@pytest.mark.parametrize('datetime, expected', [
    # Exact
    (datetime.datetime(2020, 9, 2, 5, 24, 58), True),
    # Close
    (datetime.datetime(2020, 9, 2, 5, 24, 59), True),
    (datetime.datetime(2020, 9, 2, 5, 25, 0), True),
    # Not close
    (datetime.datetime(2020, 9, 2, 7, 12, 4), False),
])
def test_does_match_date_version(datetime, expected):
    assert ScrapyLogFile(log_file_path('log1.log')).does_match_date_version(datetime) is expected


def test_errors_count():
    assert ScrapyLogFile(log_file_path('log1.log')).errors_count == 1


@pytest.mark.parametrize('filename, expected', [
    ('log1.log', False),
    ('log_sample1.log', True),
    ('log_from_date1.log', True),
])
def test_is_subset(filename, expected):
    assert ScrapyLogFile(log_file_path(filename)).is_subset() is expected


@pytest.mark.parametrize('filename, expected', [
    ('log1.log', True),
    ('log_sample1.log', True),
    ('log_from_date1.log', True),
    ('log_sigint1.log', False),
    ('log_in_progress1.log', False),
])
def test_is_finished(filename, expected):
    assert ScrapyLogFile(log_file_path(filename)).is_finished() is expected
