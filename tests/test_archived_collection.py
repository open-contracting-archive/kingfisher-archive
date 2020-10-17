import pytest

from ocdskingfisherarchive.archived_collection import _find_latest_year_month_to_load


@pytest.mark.parametrize('year, expected_year, expected_month', [
    (2020, 2020, 1),
    (2021, 2020, 1),
    (2019, None, None),
])
def test_find_latest_year_month_to_load_same_year(year, expected_year, expected_month):
    data = {
        2020: {1: True}
    }
    year, month = _find_latest_year_month_to_load(data, year, 10)
    assert year == expected_year
    assert month == expected_month
