import datetime
import os
import random
import tempfile

from ocdskingfisherarchive.crawl import Crawl
from ocdskingfisherarchive.database_archive import DataBaseArchive


def test_get_and_set():
    database_file = os.path.join(tempfile.gettempdir(), f'ocdskingfisherarchive{random.randint(0, 100000000)}.sqlite')
    while os.path.exists(database_file):
        database_file = os.path.join(tempfile.gettempdir(),
                                     f'ocdskingfisherarchive{random.randint(0, 100000000)}.sqlite')

    database_archive = DataBaseArchive(database_file)
    crawl = Crawl('scotland', datetime.datetime(2020, 9, 2, 5, 24, 58))

    # get something that doesn't exist
    assert database_archive.get_state_of_crawl(crawl) == 'UNKNOWN'

    # Set
    database_archive.set_state_of_crawl(crawl, 'CATS')
    assert database_archive.get_state_of_crawl(crawl) == 'CATS'

    # Get
    assert database_archive.get_state_of_crawl(crawl) == 'CATS'

    # Set again (to make sure replace works)
    database_archive.set_state_of_crawl(crawl, 'DOGS')
    assert database_archive.get_state_of_crawl(crawl) == 'DOGS'

    # Get
    assert database_archive.get_state_of_crawl(crawl) == 'DOGS'
