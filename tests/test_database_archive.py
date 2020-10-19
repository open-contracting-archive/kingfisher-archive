import os
import random
import tempfile

from ocdskingfisherarchive.crawl import Crawl
from ocdskingfisherarchive.database import Database


def test_get_and_set():
    database_file = os.path.join(tempfile.gettempdir(), f'ocdskingfisherarchive{random.randint(0, 100000000)}.sqlite')
    while os.path.exists(database_file):
        database_file = os.path.join(tempfile.gettempdir(),
                                     f'ocdskingfisherarchive{random.randint(0, 100000000)}.sqlite')

    database = Database(database_file)
    crawl = Crawl('scotland', '20200902_052458')

    # get something that doesn't exist
    assert database.get_state(crawl) == 'UNKNOWN'

    # Set
    database.set_state(crawl, 'CATS')
    assert database.get_state(crawl) == 'CATS'

    # Get
    assert database.get_state(crawl) == 'CATS'

    # Set again (to make sure replace works)
    database.set_state(crawl, 'DOGS')
    assert database.get_state(crawl) == 'DOGS'

    # Get
    assert database.get_state(crawl) == 'DOGS'
