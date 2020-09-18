import datetime

import pytest

from ocdskingfisherarchive.collection import Collection
from ocdskingfisherarchive.config import Config

get_s3_directory_data = [
    ("test", datetime.datetime(2020, 9, 1, 12, 0), 'test/2020/09'),
    ("test", datetime.datetime(2020, 11, 1, 12, 0), 'test/2020/11'),
]


@pytest.mark.parametrize(
    "source_id,data_version,expected_output",
    get_s3_directory_data,
)
def test_get_s3_directory(source_id, data_version, expected_output):
    config = Config()
    collection = Collection(
        config=config,
        database_id=1,
        source_id=source_id,
        data_version=data_version,
    )

    assert expected_output == collection.get_s3_directory()
