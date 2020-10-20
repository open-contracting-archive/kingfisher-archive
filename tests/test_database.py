import datetime

from ocdskingfisherarchive.crawl import Crawl
from ocdskingfisherarchive.database import Database


def test_get_and_set(tmpdir):
    crawl = Crawl(tmpdir, 'scotland', datetime.datetime(2020, 9, 2, 5, 24, 58), None)

    # Initialize.
    Database(str(tmpdir.join('db.sqlite3')))

    # Initialize existing.
    database = Database(str(tmpdir.join('db.sqlite3')))

    # Get.
    assert database.get_state(crawl) is None

    # Set and get.
    database.set_state(crawl, 'CATS')
    assert database.get_state(crawl) == 'CATS'

    # Set and get existing.
    database.set_state(crawl, 'DOGS')
    assert database.get_state(crawl) == 'DOGS'
