import os
import random
import tempfile

from ocdskingfisherarchive.config import Config
from ocdskingfisherarchive.database_archive import DataBaseArchive


def test_get_and_set():
    config = Config()
    config.database_archive_filepath = os.path.join(tempfile.gettempdir(), 'ocdskingfisherarchive' +
                                                    str(random.randint(0, 100000000))+'.sqlite')
    while os.path.exists(config.database_archive_filepath):
        config.database_archive_filepath = os.path.join(tempfile.gettempdir(), 'ocdskingfisherarchive' +
                                                        str(random.randint(0, 100000000))+'.sqlite')

    database_archive = DataBaseArchive(config)

    # get something that doesn't exist
    assert 'UNKNOWN' == database_archive.get_state_of_collection_id(1)

    # Set
    database_archive.set_state_of_collection_id(1, 'CATS')
    assert 'CATS' == database_archive.get_state_of_collection_id(1)

    # Get
    assert 'CATS' == database_archive.get_state_of_collection_id(1)

    # Set again (to make sure replace works)
    database_archive.set_state_of_collection_id(1, 'DOGS')
    assert 'DOGS' == database_archive.get_state_of_collection_id(1)

    # Get
    assert 'DOGS' == database_archive.get_state_of_collection_id(1)
