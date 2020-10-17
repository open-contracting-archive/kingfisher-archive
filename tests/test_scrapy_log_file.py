import datetime
import os

from ocdskingfisherarchive.scrapy_log_file import ScrapyLogFile
from tests import path


def test_does_match_date_version():
    scrapy_log_file = ScrapyLogFile(path('log1.log'))

    # Exact
    assert scrapy_log_file.does_match_date_version(datetime.datetime(2020, 9, 2, 5, 24, 58))

    # Close
    assert scrapy_log_file.does_match_date_version(datetime.datetime(2020, 9, 2, 5, 24, 59))
    assert scrapy_log_file.does_match_date_version(datetime.datetime(2020, 9, 2, 5, 25, 00))

    # Not Close
    assert not scrapy_log_file.does_match_date_version(datetime.datetime(2020, 9, 2, 7, 12, 4))


def test_errors_sent_to_process_count():
    scrapy_log_file = ScrapyLogFile(path('log1.log'))
    assert scrapy_log_file.get_errors_sent_to_process_count() == 1


def test_is_subset_1():
    scrapy_log_file = ScrapyLogFile(path('log1.log'))
    assert not scrapy_log_file.is_subset()


def test_is_subset_sample_1():
    scrapy_log_file = ScrapyLogFile(path('log_sample1.log'))
    assert scrapy_log_file.is_subset()


def test_is_subset_from_date_1():
    scrapy_log_file = ScrapyLogFile(path('log_from_date1.log'))
    assert scrapy_log_file.is_subset()


def test_is_finished_1():
    scrapy_log_file = ScrapyLogFile(path('log1.log'))
    assert scrapy_log_file.is_finished()


def test_is_finished_sample_1():
    scrapy_log_file = ScrapyLogFile(path('log_sample1.log'))
    assert scrapy_log_file.is_finished()


def test_is_finished_from_date_1():
    scrapy_log_file = ScrapyLogFile(path('log_from_date1.log'))
    assert scrapy_log_file.is_finished()


def test_is_finished_sigint_1():
    scrapy_log_file = ScrapyLogFile(path('log_sigint1.log'))
    assert not scrapy_log_file.is_finished()


def test_is_finished_in_progress_1():
    scrapy_log_file = ScrapyLogFile(path('log_in_progress1.log'))
    assert not scrapy_log_file.is_finished()
