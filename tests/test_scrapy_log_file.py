import datetime

import pytest

from ocdskingfisherarchive.scrapy_log_file import ScrapyLogFile
from tests import path


@pytest.mark.parametrize('datetime, expected', [
    (datetime.datetime(2020, 9, 2, 5, 24, 55), False),
    (datetime.datetime(2020, 9, 2, 5, 24, 56), False),
    (datetime.datetime(2020, 9, 2, 5, 24, 57), False),
    (datetime.datetime(2020, 9, 2, 5, 24, 58), True), # exact
    (datetime.datetime(2020, 9, 2, 5, 24, 59), True),
    (datetime.datetime(2020, 9, 2, 5, 25, 0), True),
    (datetime.datetime(2020, 9, 2, 5, 25, 1), False),
])
def test_match(datetime, expected):
    assert ScrapyLogFile(path('log_error1.log')).match(datetime) is expected


def test_errors_count():
    assert ScrapyLogFile(path('log_error1.log')).errors_count == 1


@pytest.mark.parametrize('filename, expected', [
    ('log_error1.log', False),
    ('log_sample1.log', True),
    ('log_from_date1.log', True),
])
def test_is_subset(filename, expected):
    assert ScrapyLogFile(path(filename)).is_subset() is expected


@pytest.mark.parametrize('filename, expected', [
    ('log_error1.log', True),
    ('log_sample1.log', True),
    ('log_from_date1.log', True),
    ('log_sigint1.log', False),
    ('log_in_progress1.log', False),
])
def test_is_finished(filename, expected):
    assert ScrapyLogFile(path(filename)).is_finished() is expected
