from tests import archive_fixture, collection_fixture


def test_no_previous():
    """ No Previous collections have been archived. We should archive. """
    archive = archive_fixture()
    archive._get_exact_archived_collection = lambda c: None
    archive._get_last_archived_collection = lambda c: None

    collection = collection_fixture()

    assert archive.should_we_archive_collection(collection)


def test_no_previous_subset():
    """ No Previous collections have been archived. But this is a subset, so we should not archive. """
    archive = archive_fixture()
    archive._get_exact_archived_collection = lambda c: None
    archive._get_last_archived_collection = lambda c: None

    collection = collection_fixture(spider_arguments={'sample': 'true'})

    assert not archive.should_we_archive_collection(collection)


def test_no_data_files():
    """ No Previous collections have been archived. But there are no data files, so we should not archive. """
    archive = archive_fixture()
    archive._get_exact_archived_collection = lambda c: None
    archive._get_last_archived_collection = lambda c: None

    collection = collection_fixture(data_exists=False)

    assert not archive.should_we_archive_collection(collection)


def test_scrapy_log_file_says_not_finished():
    """ Scrapy log says it is not finished so should not archive """
    archive = archive_fixture()
    archive._get_exact_archived_collection = lambda c: None
    archive._get_last_archived_collection = lambda c: None

    collection = collection_fixture(is_finished=False)

    assert not archive.should_we_archive_collection(collection)
