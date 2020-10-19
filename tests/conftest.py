import os

import pytest

from ocdskingfisherarchive.archive import Archive


@pytest.fixture
def archive(tmpdir):
    return Archive(
        os.getenv('KINGFISHER_ARCHIVE_BUCKET_NAME'),
        tmpdir.join('data'),
        tmpdir.join('logs', 'kingfisher'),
        str(tmpdir.join('db.sqlite3')),
    )
