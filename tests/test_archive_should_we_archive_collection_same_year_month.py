import unittest
from unittest.mock import patch

from tests import archive_fixture, collection_fixture


@patch('ocdskingfisherarchive.s3.S3.load_latest', return_value=(None, None, None))
class SamePeriod(unittest.TestCase):
    @patch('ocdskingfisherarchive.s3.S3.load_exact')
    def test_backup(self, load_exact, load_latest):
        """
        This source was archived this year/month. Local Collection is bigger and with a different MD5 so backup.
        """
        load_exact.return_value = {'data_md5': 'oeu7394ud48h', 'data_size': 123456, 'errors_count': None}
        archive = archive_fixture()
        collection = collection_fixture(size=186306)

        assert archive.should_we_archive_collection(collection)

    @patch('ocdskingfisherarchive.s3.S3.load_exact')
    def test_same_md5(self, load_exact, load_latest):
        """
        This source was archived this year/month. MD5 is the same so don't back up.
        """
        load_exact.return_value = {'data_md5': 'eo39tj38jm', 'data_size': 123456, 'errors_count': None}
        archive = archive_fixture()
        collection = collection_fixture(size=186306)

        assert not archive.should_we_archive_collection(collection)

    @patch('ocdskingfisherarchive.s3.S3.load_exact')
    def test_smaller_size(self, load_exact, load_latest):
        """
        This source was archived this year/month. Local Collection is smaller, so don't backup.
        """
        load_exact.return_value = {'data_md5': 'oeu7394ud48h', 'data_size': 234813, 'errors_count': None}
        archive = archive_fixture()
        collection = collection_fixture(size=186306)

        assert not archive.should_we_archive_collection(collection)

    @patch('ocdskingfisherarchive.s3.S3.load_exact')
    def test_more_errors(self, load_exact, load_latest):
        """
        This source was archived this year/month. Local Collection has more errors, so don't backup.
        """
        load_exact.return_value = {'data_md5': 'oeu7394ud48h', 'data_size': 123456, 'errors_count': 0}
        archive = archive_fixture()
        collection = collection_fixture(size=186306, errors_count=1)

        assert not archive.should_we_archive_collection(collection)
