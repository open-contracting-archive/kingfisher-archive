import datetime

from ocdskingfisherarchive.cache import Cache
from ocdskingfisherarchive.crawl import Crawl


def test_get_and_set(tmpdir):
    crawl = Crawl(tmpdir, 'scotland', datetime.datetime(2020, 9, 2, 5, 24, 58), None)

    # Initialize.
    Cache(str(tmpdir.join('cache.sqlite3')))

    # Initialize existing.
    cache = Cache(str(tmpdir.join('cache.sqlite3')))

    # Get.
    assert cache.get(crawl) is None

    # Set and get.
    cache.set(crawl, 'CATS')
    assert cache.get(crawl) == 'CATS'

    # Set and get existing.
    cache.set(crawl, 'DOGS')
    assert cache.get(crawl) == 'DOGS'
