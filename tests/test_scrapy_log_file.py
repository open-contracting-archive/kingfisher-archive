import datetime
import os

from ocdskingfisherarchive.scrapy_log_file import ScrapyLogFile


def test_log_1():

    log_filename = os.path.join(os.path.dirname(
        os.path.realpath(__file__)), 'data', 'log1.log'
    )

    scrapy_log_file = ScrapyLogFile(log_filename)

    # Exact
    assert True == scrapy_log_file.does_match_date_version(  # noqa: E712
        datetime.datetime(2020, 9, 2, 5, 24, 58)
    )

    # Close
    assert True == scrapy_log_file.does_match_date_version(  # noqa: E712
        datetime.datetime(2020, 9, 2, 5, 24, 59)
    )
    assert True == scrapy_log_file.does_match_date_version(  # noqa: E712
        datetime.datetime(2020, 9, 2, 5, 25, 00)
    )

    # Not Close
    assert False == scrapy_log_file.does_match_date_version(  # noqa: E712
        datetime.datetime(2020, 9, 2, 7, 12, 4)
    )
