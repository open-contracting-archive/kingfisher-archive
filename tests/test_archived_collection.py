from ocdskingfisherarchive.archived_collection import _find_latest_year_month_to_load


def test_find_latest_year_month_to_load__same_year():
    data = {
        2020: {1: True}
    }
    year, month = _find_latest_year_month_to_load(data, 2020, 10)
    assert year == 2020
    assert month == 1


def test_find_latest_year_month_to_load__last_year():
    data = {
        2020: {1: True}
    }
    year, month = _find_latest_year_month_to_load(data, 2021, 10)
    assert year == 2020
    assert month == 1


def test_find_latest_year_month_to_load__not_found():
    data = {
        2020: {1: True}
    }
    year, month = _find_latest_year_month_to_load(data, 2019, 10)
    assert year is None
    assert month is None
