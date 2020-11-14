import os

import pytest

from ocdskingfisherarchive.archive import Archiver


@pytest.fixture
def archiver(tmpdir):
    return Archiver(
        os.getenv('KINGFISHER_ARCHIVE_BUCKET_NAME'),
        tmpdir.join('data'),
        tmpdir.join('logs', 'kingfisher'),
        str(tmpdir.join('cache.sqlite3')),
    )
