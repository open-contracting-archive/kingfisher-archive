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
    assert database_archive.get_state_of_collection_id(1) == 'UNKNOWN'

    # Set
    database_archive.set_state_of_collection_id(1, 'CATS')
    assert database_archive.get_state_of_collection_id(1) == 'CATS'

    # Get
    assert database_archive.get_state_of_collection_id(1) == 'CATS'

    # Set again (to make sure replace works)
    database_archive.set_state_of_collection_id(1, 'DOGS')
    assert database_archive.get_state_of_collection_id(1) == 'DOGS'

    # Get
    assert database_archive.get_state_of_collection_id(1) == 'DOGS'
