import os
import random
import tempfile

from ocdskingfisherarchive.database_archive import DataBaseArchive


def test_get_and_set():
    database_file = os.path.join(tempfile.gettempdir(), f'ocdskingfisherarchive{random.randint(0, 100000000)}.sqlite')
    while os.path.exists(database_file):
        database_file = os.path.join(tempfile.gettempdir(),
                                     f'ocdskingfisherarchive{random.randint(0, 100000000)}.sqlite')

    database_archive = DataBaseArchive(database_file)

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
