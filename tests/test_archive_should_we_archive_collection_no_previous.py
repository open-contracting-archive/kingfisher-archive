import unittest
from unittest.mock import patch

from tests import archive_fixture, collection_fixture


@patch('ocdskingfisherarchive.s3.S3.load_latest', return_value=None)
@patch('ocdskingfisherarchive.s3.S3.load_exact', return_value=None)
class NoPeriod(unittest.TestCase):
    def test_no_previous(self, load_exact, load_latest):
        """
        No previous collections have been archived. We should archive.
        """
        collection = collection_fixture()

        assert archive_fixture().should_we_archive_collection(collection)

    def test_no_previous_subset(self, load_exact, load_latest):
        """
        No previous collections have been archived. But this is a subset, so we should not archive.
        """
        collection = collection_fixture(spider_arguments={'sample': 'true'})

        assert not archive_fixture().should_we_archive_collection(collection)

    def test_no_data_files(self, load_exact, load_latest):
        """
        No previous collections have been archived. But there are no data files, so we should not archive.
        """
        collection = collection_fixture(data_exists=False)

        assert not archive_fixture().should_we_archive_collection(collection)

    def test_scrapy_log_file_says_not_finished(self, load_exact, load_latest):
        """
        Scrapy log says it is not finished so should not archive
        """
        collection = collection_fixture(is_finished=False)

        assert not archive_fixture().should_we_archive_collection(collection)
