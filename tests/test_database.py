from ocdskingfisherarchive.crawl import Crawl
from ocdskingfisherarchive.database import Database


def test_get_and_set(tmpdir):
    database = Database(tmpdir.join('db.sqlite3'))
    crawl = Crawl('scotland', '20200902_052458')

    assert database.get_state(crawl) == 'UNKNOWN'

    # Set and get.
    database.set_state(crawl, 'CATS')
    assert database.get_state(crawl) == 'CATS'

    # Set and get existing.
    database.set_state(crawl, 'DOGS')
    assert database.get_state(crawl) == 'DOGS'
