import datetime

from ocdskingfisherarchive.crawl import Crawl
from ocdskingfisherarchive.database import Database


def test_get_and_set(tmpdir):
    database = Database(str(tmpdir.join('db.sqlite3')))
    crawl = Crawl(tmpdir, 'scotland', datetime.datetime(2020, 9, 2, 5, 24, 58), None)

    assert database.get_state(crawl) is None

    # Set and get.
    database.set_state(crawl, 'CATS')
    assert database.get_state(crawl) == 'CATS'

    # Set and get existing.
    database.set_state(crawl, 'DOGS')
    assert database.get_state(crawl) == 'DOGS'
