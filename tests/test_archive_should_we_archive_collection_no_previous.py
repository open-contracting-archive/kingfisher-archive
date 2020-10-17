from tests import archive_fixture, collection_fixture


def test_no_previous():
    """
    No previous collections have been archived. We should archive.
    """
    collection = collection_fixture()

    assert archive_fixture().should_we_archive_collection(collection)


def test_no_previous_subset():
    """
    No previous collections have been archived. But this is a subset, so we should not archive.
    """
    collection = collection_fixture(spider_arguments={'sample': 'true'})

    assert not archive_fixture().should_we_archive_collection(collection)


def test_no_data_files():
    """
    No previous collections have been archived. But there are no data files, so we should not archive.
    """
    collection = collection_fixture(data_exists=False)

    assert not archive_fixture().should_we_archive_collection(collection)


def test_scrapy_log_file_says_not_finished():
    """
    Scrapy log says it is not finished so should not archive
    """
    collection = collection_fixture(is_finished=False)

    assert not archive_fixture().should_we_archive_collection(collection)
