import datetime

import pytest

from ocdskingfisherarchive.collection import Collection


@pytest.mark.parametrize('source_id, data_version, expected_output', [
    ('test', datetime.datetime(2020, 9, 1, 12, 0), 'test/2020/09'),
    ('test', datetime.datetime(2020, 11, 1, 12, 0), 'test/2020/11'),
])
def test_get_s3_directory(source_id, data_version, expected_output):
    collection = Collection(1, source_id, data_version)

    assert collection.get_s3_directory() == expected_output
