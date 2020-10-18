import unittest
from unittest.mock import patch

import pytest

from ocdskingfisherarchive.archived_collection import ArchivedCollection
from tests import archive_fixture, collection_fixture


@patch('ocdskingfisherarchive.archived_collection.load_exact', return_value=None)
class EarlierPeriod(unittest.TestCase):
    def assert_log(self, levelname, message):
        assert len(self.caplog.records) == 1
        assert self.caplog.records[0].name == 'ocdskingfisher.archive'
        assert self.caplog.records[0].levelname == levelname, f'{self.caplog.records[0].levelname!r} == {levelname!r}'
        assert self.caplog.records[0].message == message, f'{self.caplog.records[0].message!r} == {message!r}'

    @pytest.fixture(autouse=True)
    def fixtures(self, caplog):
        self.caplog = caplog

    @patch('ocdskingfisherarchive.archived_collection.load_latest')
    def test_backup(self, load_latest, load_exact):
        """
        This source was archived before this month. We should archive.
        """
        load_latest.return_value = ArchivedCollection({'data_md5': 'oeu7394ud48h', 'data_size': 123456}, 2020, 9)
        archive = archive_fixture()
        collection = collection_fixture(size=386306)

        assert archive.should_we_archive_collection(collection)
        self.assert_log('INFO', 'Archiving 1 because an archive exists from earlier period (2020/9) and local '
                                'collection has 50% more size')

    @patch('ocdskingfisherarchive.archived_collection.load_latest')
    def test_backup_zero_errors_slightly_bigger_size(self, load_latest, load_exact):
        """
        This source was archived before this month. Both collections have zero errors, but local is slightly bigger
        (not 50% bigger). We should archive.
        """
        load_latest.return_value = ArchivedCollection(
            {'data_md5': 'oeu7394ud48h', 'data_size': 123456, 'errors_count': 0}, 2020, 9
        )
        archive = archive_fixture()
        collection = collection_fixture(size=123457)

        assert archive.should_we_archive_collection(collection)
        self.assert_log('INFO', 'Archiving 1 because an archive exists from earlier period (2020/9) and local '
                                'collection has fewer or equal errors and greater or equal size')

    @patch('ocdskingfisherarchive.archived_collection.load_latest')
    def test_same_md5(self, load_latest, load_exact):
        """
        This source was archived before this month. MD5 is the same so don't back up.
        """
        load_latest.return_value = ArchivedCollection({'data_md5': 'eo39tj38jm', 'data_size': 123456}, 2020, 1)
        archive = archive_fixture()
        collection = collection_fixture(size=386306)

        assert not archive.should_we_archive_collection(collection)
        self.assert_log('INFO', 'Skipping 1 because an archive exists from earlier period (2020/1) and same MD5')

    @patch('ocdskingfisherarchive.archived_collection.load_latest')
    def test_size_not_50_percent_more(self, load_latest, load_exact):
        """
        This source was archived before this month. The size is slightly larger but not 50% so don't back up.
        """
        load_latest.return_value = ArchivedCollection({'data_md5': 'oeu7394ud48h', 'data_size': 123456}, 2020, 1)
        archive = archive_fixture()
        collection = collection_fixture(size=133456)

        assert not archive.should_we_archive_collection(collection)
        self.assert_log('INFO', 'Skipping 1 because an archive exists from earlier period (2020/1) and we can not '
                                'find a good reason to backup')

    @patch('ocdskingfisherarchive.archived_collection.load_latest')
    def test_less_errors(self, load_latest, load_exact):
        """
        This source was archived before this month. Local collection has fewer errors, so backup.
        """
        load_latest.return_value = ArchivedCollection(
            {'data_md5': 'oeu7394ud48h', 'data_size': 123456, 'errors_count': 1}, 2020, 9
        )
        archive = archive_fixture()
        collection = collection_fixture(size=123456)

        assert archive.should_we_archive_collection(collection)
        self.assert_log('INFO', 'Archiving 1 because an archive exists from earlier period (2020/9) and local '
                                'collection has fewer or equal errors and greater or equal size')

    @patch('ocdskingfisherarchive.archived_collection.load_latest')
    def test_less_errors_but_smaller_size(self, load_latest, load_exact):
        """
        This source was archived before this month. Local collection has fewer errors but is smaller size, so don't
        backup.
        """
        load_latest.return_value = ArchivedCollection(
            {'data_md5': 'oeu7394ud48h', 'data_size': 123456, 'errors_count': 1}, 2020, 9
        )
        archive = archive_fixture()
        collection = collection_fixture(size=100456)

        assert not archive.should_we_archive_collection(collection)
        self.assert_log('INFO', 'Skipping 1 because an archive exists from earlier period (2020/9) and we can not '
                                'find a good reason to backup')
