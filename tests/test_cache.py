import datetime

from ocdskingfisherarchive.cache import Cache
from ocdskingfisherarchive.crawl import Crawl


def test_get_and_set(tmpdir):
    expected = {
        'bytes': 0,
        'checksum': '99aa06d3014798d86001c324468d497f',
        'errors_count': None,
        'files_count': None,
    }

    crawl = Crawl('scotland', '20200902_052458', tmpdir, None)

    # Initialize.
    Cache(str(tmpdir.join('cache.sqlite3')))

    # Initialize existing.
    cache = Cache(str(tmpdir.join('cache.sqlite3')))

    # Get.
    assert cache.get(crawl) == (None, None)

    # Set and get.
    cache.set(crawl, True)
    crawl, archived = cache.get(crawl)

    assert crawl.source_id == 'scotland'
    assert crawl.data_version == datetime.datetime(2020, 9, 2, 5, 24, 58)
    assert crawl._cache == expected
    assert archived is True

    # Set and get existing.
    cache.set(crawl, False)
    crawl, archived = cache.get(crawl)

    assert crawl.source_id == 'scotland'
    assert crawl.data_version == datetime.datetime(2020, 9, 2, 5, 24, 58)
    assert crawl._cache == expected
    assert archived is False
