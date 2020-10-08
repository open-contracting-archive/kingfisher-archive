import datetime
import os

from ocdskingfisherarchive.scrapy_log_file import ScrapyLogFile


FILENAME_LOG1 = os.path.join(os.path.dirname(
        os.path.realpath(__file__)), 'data', 'log1.log'
    )
FILENAME_LOG_SAMPLE1 = os.path.join(os.path.dirname(
        os.path.realpath(__file__)), 'data', 'log_sample1.log'
    )

FILENAME_LOG_FROM_DATE1 = os.path.join(os.path.dirname(
        os.path.realpath(__file__)), 'data', 'log_from_date1.log'
    )

FILENAME_LOG_SIGINT1 = os.path.join(os.path.dirname(
        os.path.realpath(__file__)), 'data', 'log_sigint1.log'
    )

FILENAME_LOG_IN_PROGRESS1 = os.path.join(os.path.dirname(
        os.path.realpath(__file__)), 'data', 'log_in_progress1.log'
    )


def test_does_match_date_version():
    scrapy_log_file = ScrapyLogFile(FILENAME_LOG1)

    # Exact
    assert scrapy_log_file.does_match_date_version(datetime.datetime(2020, 9, 2, 5, 24, 58)) is True

    # Close
    assert scrapy_log_file.does_match_date_version(datetime.datetime(2020, 9, 2, 5, 24, 59)) is True
    assert scrapy_log_file.does_match_date_version(datetime.datetime(2020, 9, 2, 5, 25, 00)) is True

    # Not Close
    assert scrapy_log_file.does_match_date_version(datetime.datetime(2020, 9, 2, 7, 12, 4)) is False


def test_errors_sent_to_process_count():
    scrapy_log_file = ScrapyLogFile(FILENAME_LOG1)
    assert 1 == scrapy_log_file.get_errors_sent_to_process_count()


def test_is_subset_1():
    scrapy_log_file = ScrapyLogFile(FILENAME_LOG1)
    assert scrapy_log_file.is_subset() is False


def test_is_subset_sample_1():
    scrapy_log_file = ScrapyLogFile(FILENAME_LOG_SAMPLE1)
    assert scrapy_log_file.is_subset() is True


def test_is_subset_from_date_1():
    scrapy_log_file = ScrapyLogFile(FILENAME_LOG_FROM_DATE1)
    assert scrapy_log_file.is_subset() is True


def test_is_finished_1():
    scrapy_log_file = ScrapyLogFile(FILENAME_LOG1)
    assert scrapy_log_file.is_finished() is True


def test_is_finished_sample_1():
    scrapy_log_file = ScrapyLogFile(FILENAME_LOG_SAMPLE1)
    assert scrapy_log_file.is_finished() is True


def test_is_finished_from_date_1():
    scrapy_log_file = ScrapyLogFile(FILENAME_LOG_FROM_DATE1)
    assert scrapy_log_file.is_finished() is True


def test_is_finished_sigint_1():
    scrapy_log_file = ScrapyLogFile(FILENAME_LOG_SIGINT1)
    assert scrapy_log_file.is_finished() is False


def test_is_finished_in_progress_1():
    scrapy_log_file = ScrapyLogFile(FILENAME_LOG_IN_PROGRESS1)
    assert scrapy_log_file.is_finished() is False
