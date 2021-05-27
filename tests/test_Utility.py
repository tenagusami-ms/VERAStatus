from datetime import datetime

from VERAStatus.Utility import in_jst, UTC, JST, incremented_day, decremented_day, round_float, doy2datetime, \
    datetime2year_doy_string, datetime2year_doy, datetime2doy_string, datetime2doy, string_lines2string, \
    wind_direction2octas


def test_in_jst():
    assert in_jst(datetime(2020, 10, 31, 0, 0, 0, tzinfo=UTC)) \
           == datetime(2020, 10, 31, 9, 0, 0, tzinfo=JST)


def test_incremented_day():
    assert incremented_day(datetime(2020, 10, 31, 0, 0, 0, tzinfo=UTC)) == \
           datetime(2020, 11, 1, 0, 0, 0, tzinfo=UTC)
    assert incremented_day(datetime(2020, 10, 31, 0, 0, 0, tzinfo=JST)) == \
           datetime(2020, 11, 1, 0, 0, 0, tzinfo=JST)


def test_decremented_day():
    assert decremented_day(datetime(2020, 10, 31, 0, 0, 0, tzinfo=UTC)) == \
           datetime(2020, 10, 30, 0, 0, 0, tzinfo=UTC)
    assert decremented_day(datetime(2020, 10, 31, 0, 0, 0, tzinfo=JST)) == \
           datetime(2020, 10, 30, 0, 0, 0, tzinfo=JST)


def test_round_float():
    assert round_float(0.15, -1) == 0.2
    assert round_float(0.149999, -1) == 0.1
    assert round_float(12.49999, 0) == 12
    assert round_float(12.5, 0) == 13


def test_doy2datetime():
    assert doy2datetime(2020, 300) == datetime(2020, 10, 26, 0, 0, 0, tzinfo=UTC)
    assert doy2datetime(2020, 1) == datetime(2020, 1, 1, 0, 0, 0, tzinfo=UTC)


def test_datetime2year_doy_string():
    assert datetime2year_doy_string(datetime(2020, 10, 26, 0, 0, 0, tzinfo=UTC)) == ("2020", "300")
    assert datetime2year_doy_string(datetime(2020, 1, 1, 10, 30, 0, tzinfo=UTC)) == ("2020", "001")


def test_datetime2year_doy():
    assert datetime2year_doy(datetime(2020, 10, 26, 0, 0, 0, tzinfo=UTC)) == (2020, 300)
    assert datetime2year_doy(datetime(2020, 1, 1, 10, 30, 0, tzinfo=UTC)) == (2020, 1)


def test_datetime2doy_string():
    assert datetime2doy_string(datetime(2020, 10, 26, 0, 0, 0, tzinfo=UTC)) == "2020300"
    assert datetime2doy_string(datetime(2020, 1, 1, 10, 30, 0, tzinfo=UTC)) == "2020001"


def test_datetime2doy():
    assert datetime2doy(datetime(2020, 10, 26, 0, 0, 0, tzinfo=UTC)) == 300
    assert datetime2doy(datetime(2020, 1, 1, 10, 30, 0, tzinfo=UTC)) == 1


def test_string_lines2string():
    assert string_lines2string(["line1", "line2", "line3"]) == "line1\nline2\nline3\n"


def test_wind_direction2octas():
    assert wind_direction2octas(45.0) == "NE"
    assert wind_direction2octas(-50.0) == "NW"
    assert wind_direction2octas(500.0) == "SE"
