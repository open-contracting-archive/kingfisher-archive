from tests import archive_fixture, collection_fixture


def test_backup():
    """
    This source was archived this year/month. Local Collection is bigger and with a different MD5 so backup.
    """
    archive = archive_fixture(exact=({'data_md5': 'oeu7394ud48h', 'data_size': 123456}, 2020, 9))
    collection = collection_fixture(size=186306)

    assert archive.should_we_archive_collection(collection)


def test_same_md5():
    """
    This source was archived this year/month. MD5 is the same so don't back up.
    """
    archive = archive_fixture(exact=({'data_md5': 'eo39tj38jm', 'data_size': 123456}, 2020, 9))
    collection = collection_fixture(size=186306)

    assert not archive.should_we_archive_collection(collection)


def test_smaller_size():
    """
    This source was archived this year/month. Local Collection is smaller, so don't backup.
    """
    archive = archive_fixture(exact=({'data_md5': 'oeu7394ud48h', 'data_size': 234813}, 2020, 9))
    collection = collection_fixture(size=186306)

    assert not archive.should_we_archive_collection(collection)


def test_more_errors():
    """
    This source was archived this year/month. Local Collection has more errors, so don't backup.
    """
    archive = archive_fixture(exact=(
        {'data_md5': 'oeu7394ud48h', 'data_size': 123456, 'scrapy_log_file_found': True, 'errors_count': 0}, 2020, 9
    ))
    collection = collection_fixture(size=186306, errors_count=1)

    assert not archive.should_we_archive_collection(collection)
