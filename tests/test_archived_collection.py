import pytest

from ocdskingfisherarchive.archived_collection import _find_latest_year_month_to_load


@pytest.mark.parametrize('year, expected_year, expected_month', [
    (2020, 2020, 1),
    (2021, 2020, 1),
    (2019, None, None),
])
def test_find_latest_year_month_to_load_same_year(year, expected_year, expected_month):
    actual_year, actual_month = _find_latest_year_month_to_load({2020: {1: True}}, year, 10)

    assert actual_year == expected_year
    assert actual_month == expected_month
