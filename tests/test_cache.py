import datetime

import attr

from ocdskingfisherarchive.cache import Cache
from ocdskingfisherarchive.crawl import Crawl


def test_get_and_set(tmpdir):
    expected = {
        'version': '1',
        'source_id': 'scotland',
        'data_version': '2020-09-02 05:24:58',
        'bytes': 0,
        'checksum': '99aa06d3014798d86001c324468d497f',
        'errors_count': None,
        'files_count': None,
    }

    crawl = Crawl(tmpdir, 'scotland', datetime.datetime(2020, 9, 2, 5, 24, 58), None)

    # Initialize.
    Cache(str(tmpdir.join('cache.sqlite3')))

    # Initialize existing.
    cache = Cache(str(tmpdir.join('cache.sqlite3')))

    # Get.
    assert cache.get(crawl) == (None, None)

    # Set and get.
    cache.set(crawl, True)
    metadata, archived = cache.get(crawl)
    assert attr.asdict(metadata) == expected
    assert archived is True

    # Set and get existing.
    cache.set(crawl, False)
    metadata, archived = cache.get(crawl)
    assert attr.asdict(metadata) == expected
    assert archived is False
