from ocdskingfisherarchive.cache import Cache
from ocdskingfisherarchive.crawl import Crawl


def test_cache(tmpdir):
    query = Crawl('scotland', '20200902_052458')

    crawl = Crawl('scotland', '20200902_052458', tmpdir, None)
    crawl.reject_reason

    # Initialize.
    Cache(str(tmpdir.join('cache.sqlite3')))

    # Initialize existing.
    cache = Cache(str(tmpdir.join('cache.sqlite3')))

    # Get.
    assert cache.get(query) == query

    # Set and get.
    crawl.archived = True
    cache.set(crawl)
    crawl = cache.get(query)

    assert crawl.asdict() == {
        'id': 'scotland/20200902_052458',
        'source_id': 'scotland',
        'data_version': '20200902_052458',
        'bytes': None,
        'checksum': None,
        'errors_count': None,
        'files_count': None,
        'reject_reason': 'no_data_directory',
        'archived': True,
    }

    # Set and get existing.
    crawl.archived = False
    cache.set(crawl)
    crawl = cache.get(query)

    assert crawl.asdict() == {
        'id': 'scotland/20200902_052458',
        'source_id': 'scotland',
        'data_version': '20200902_052458',
        'bytes': None,
        'checksum': None,
        'errors_count': None,
        'files_count': None,
        'reject_reason': 'no_data_directory',
        'archived': False,
    }

    # Delete.
    cache.delete(crawl)

    assert cache.get(query) == query
