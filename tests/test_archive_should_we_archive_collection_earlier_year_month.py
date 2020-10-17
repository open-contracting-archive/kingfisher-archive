from tests import archive_fixture, collection_fixture


def test_backup():
    """
    This source was archived before this month. We should archive.
    """
    archive = archive_fixture(last=({'data_md5': 'oeu7394ud48h', 'data_size': 123456}, 2020, 9))
    collection = collection_fixture(size=386306)

    assert archive.should_we_archive_collection(collection)


def test_backup_zero_errors_slightly_bigger_size():
    """
    This source was archived before this month. Both collections have zero errors, but local is slightly bigger (not
    50% bigger). We should archive.
    """
    archive = archive_fixture(last=(
        {'data_md5': 'oeu7394ud48h', 'data_size': 123456, 'errors_count': 0, 'scrapy_log_file_found': True}, 2020, 9
    ))
    collection = collection_fixture(size=123457)

    assert archive.should_we_archive_collection(collection)


def test_same_md5():
    """
    This source was archived before this month.  MD5 is the same so don't back up.
    """
    archive = archive_fixture(last=({'data_md5': 'eo39tj38jm', 'data_size': 123456}, 2020, 1))
    collection = collection_fixture(size=386306)

    assert not archive.should_we_archive_collection(collection)


def test_size_not_50_percent_more():
    """
    This source was archived before this month.  The size is slightly larger but not 50% so don't back up.
    """
    archive = archive_fixture(last=({'data_md5': 'oeu7394ud48h', 'data_size': 123456}, 2020, 1))
    collection = collection_fixture(size=133456)

    assert not archive.should_we_archive_collection(collection)


def test_less_errors():
    """
    This source was archived before this month.  Local Collection has less errors, so backup.
    """
    archive = archive_fixture(last=(
        {'data_md5': 'oeu7394ud48h', 'data_size': 123456, 'scrapy_log_file_found': True, 'errors_count': 1}, 2020, 9
    ))
    collection = collection_fixture(size=123456)

    assert archive.should_we_archive_collection(collection)


def test_less_errors_but_smaller_size():
    """
    This source was archived before this month. Local Collection has less errors but is smaller size, so don't backup.
    """
    archive = archive_fixture(last=(
        {'data_md5': 'oeu7394ud48h', 'data_size': 123456, 'scrapy_log_file_found': True, 'errors_count': 1}, 2020, 9
    ))
    collection = collection_fixture(size=100456)

    assert not archive.should_we_archive_collection(collection)
