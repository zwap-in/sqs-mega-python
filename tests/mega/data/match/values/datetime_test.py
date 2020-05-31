from datetime import date, datetime

import dateutil.parser
import pytest
from dateutil.tz import tzutc, tzoffset
from parameterized import parameterized

from mega.data.match.values.datetime import DateTime
from mega.data.match.values.value import LeftHandSideTypeError, RightHandSideTypeError

UTC = tzutc()
GMT_MINUS_3 = tzoffset('FOO_BAR', -3 * 60 * 60)


@parameterized.expand([
    ['2020-05-15'],
    ['2020-05-15T16:45:20.123456'],
    [date(2020, 5, 15)]
])
def test_datetime_rhs_is_not_equal_nor_match_none_lhs(rhs):
    assert DateTime(rhs).equal(None) is False
    assert DateTime(rhs).match(None) is False


@parameterized.expand([
    [''],
    ['2020-05-15#!'],
    [True],
    [2020],
    [object()]
])
def test_datetime_does_not_accept_invalid_rhs(rhs):
    with pytest.raises(RightHandSideTypeError) as e:
        DateTime(rhs)

    assert '[DateTime] Invalid right-hand side <{}>'.format(type(rhs).__name__) in str(e.value)


@parameterized.expand([
    ['', '2020-05-15'],
    ['2020-05-15#!', '2020-05-15'],
    [True, '2020-05-15'],
    [2020, '2020-05-15'],
    [2020, dateutil.parser.parse('2020-05-15T16:45:20.123456')],
    [object(), date(2020, 5, 15)]
])
def test_datetime_does_not_accept_invalid_lhs(lhs, rhs):
    value = DateTime(rhs)
    for function in [
        DateTime.equal,
        DateTime.match,
        DateTime.less_than,
        DateTime.less_than_or_equal,
        DateTime.greater_than,
        DateTime.greater_than_or_equal
    ]:
        with pytest.raises(LeftHandSideTypeError) as e:
            function(value, lhs)

        assert (
                '[DateTime.{}] Could not apply left-hand side <{}>'.format(function.__name__, type(lhs).__name__)
                in str(e.value)
        )


def test_datetime_comparison_does_not_accept_null_lhs():
    value = DateTime('2020-03-05T15:45:29+03:00')
    for function in [
        DateTime.less_than,
        DateTime.less_than_or_equal,
        DateTime.greater_than,
        DateTime.greater_than_or_equal
    ]:
        with pytest.raises(LeftHandSideTypeError) as e:
            function(value, None)

        assert (
                '[DateTime.{}] Could not apply left-hand side <NoneType> (None)'.format(function.__name__)
                in str(e.value)
        )


@parameterized.expand([
    ['2020-05-15T16:45:20.123456', '2020-05-15T16:45:20.123456'],
    ['2020-05-15T16:45:20.123456', '2020-05-15T16:45:20.123456+00:00'],
    ['2020-05-15T16:45:20.123456', '2020-05-15T19:45:20.123456+03:00'],
    ['2020-05-15T16:45:20.123456', datetime(2020, 5, 15, 16, 45, 20, 123456)],
    ['2020-05-15T16:45:20.123456', datetime(2020, 5, 15, 16, 45, 20, 123456, tzinfo=UTC)],
    ['2020-05-15T16:45:20.123456', datetime(2020, 5, 15, 13, 45, 20, 123456, tzinfo=GMT_MINUS_3)],

    ['2020-05-15T16:45:20.123456+00:00', '2020-05-15T16:45:20.123456'],
    ['2020-05-15T16:45:20.123456+00:00', '2020-05-15T16:45:20.123456+00:00'],
    ['2020-05-15T16:45:20.123456+00:00', '2020-05-15T19:45:20.123456+03:00'],
    ['2020-05-15T16:45:20.123456+00:00', datetime(2020, 5, 15, 16, 45, 20, 123456)],
    ['2020-05-15T16:45:20.123456+00:00', datetime(2020, 5, 15, 16, 45, 20, 123456, tzinfo=UTC)],
    ['2020-05-15T16:45:20.123456+00:00', datetime(2020, 5, 15, 13, 45, 20, 123456, tzinfo=GMT_MINUS_3)],

    ['2020-05-15T11:45:20.123456-05:00', '2020-05-15T16:45:20.123456'],
    ['2020-05-15T11:45:20.123456-05:00', '2020-05-15T16:45:20.123456+00:00'],
    ['2020-05-15T11:45:20.123456-05:00', '2020-05-15T19:45:20.123456+03:00'],
    ['2020-05-15T11:45:20.123456-05:00', datetime(2020, 5, 15, 16, 45, 20, 123456)],
    ['2020-05-15T11:45:20.123456-05:00', datetime(2020, 5, 15, 16, 45, 20, 123456, tzinfo=UTC)],
    ['2020-05-15T11:45:20.123456-05:00', datetime(2020, 5, 15, 13, 45, 20, 123456, tzinfo=GMT_MINUS_3)],

    [datetime(2020, 5, 15, 16, 45, 20, 123456), '2020-05-15T16:45:20.123456'],
    [datetime(2020, 5, 15, 16, 45, 20, 123456), '2020-05-15T16:45:20.123456+00:00'],
    [datetime(2020, 5, 15, 16, 45, 20, 123456), '2020-05-15T19:45:20.123456+03:00'],
    [datetime(2020, 5, 15, 16, 45, 20, 123456), datetime(2020, 5, 15, 16, 45, 20, 123456)],
    [datetime(2020, 5, 15, 16, 45, 20, 123456), datetime(2020, 5, 15, 16, 45, 20, 123456, tzinfo=UTC)],
    [datetime(2020, 5, 15, 16, 45, 20, 123456), datetime(2020, 5, 15, 13, 45, 20, 123456, tzinfo=GMT_MINUS_3)],

    [datetime(2020, 5, 15, 16, 45, 20, 123456, tzinfo=UTC), '2020-05-15T16:45:20.123456'],
    [datetime(2020, 5, 15, 16, 45, 20, 123456, tzinfo=UTC), '2020-05-15T16:45:20.123456+00:00'],
    [datetime(2020, 5, 15, 16, 45, 20, 123456, tzinfo=UTC), '2020-05-15T19:45:20.123456+03:00'],
    [datetime(2020, 5, 15, 16, 45, 20, 123456, tzinfo=UTC), datetime(2020, 5, 15, 16, 45, 20, 123456)],
    [datetime(2020, 5, 15, 16, 45, 20, 123456, tzinfo=UTC), datetime(2020, 5, 15, 16, 45, 20, 123456, tzinfo=UTC)],
    [datetime(2020, 5, 15, 16, 45, 20, 123456, tzinfo=UTC),
     datetime(2020, 5, 15, 13, 45, 20, 123456, tzinfo=GMT_MINUS_3)],

    [datetime(2020, 5, 15, 13, 45, 20, 123456, tzinfo=GMT_MINUS_3), '2020-05-15T16:45:20.123456'],
    [datetime(2020, 5, 15, 13, 45, 20, 123456, tzinfo=GMT_MINUS_3), '2020-05-15T16:45:20.123456+00:00'],
    [datetime(2020, 5, 15, 13, 45, 20, 123456, tzinfo=GMT_MINUS_3), '2020-05-15T19:45:20.123456+03:00'],
    [datetime(2020, 5, 15, 13, 45, 20, 123456, tzinfo=GMT_MINUS_3), datetime(2020, 5, 15, 16, 45, 20, 123456)],
    [datetime(2020, 5, 15, 13, 45, 20, 123456, tzinfo=GMT_MINUS_3),
     datetime(2020, 5, 15, 16, 45, 20, 123456, tzinfo=UTC)],
    [datetime(2020, 5, 15, 13, 45, 20, 123456, tzinfo=GMT_MINUS_3),
     datetime(2020, 5, 15, 13, 45, 20, 123456, tzinfo=GMT_MINUS_3)],

    ['2020-05-15 16:45:20 Z', '2020-05-15T16:45:20.000'],
    ['2020-05-15 16:45:20 Z', '2020-05-15T16:45:20.000+00:00'],
    ['2020-05-15 16:45:20 Z', '2020-05-15T19:45:20.000+03:00'],
    ['2020-05-15 16:45:20 Z', datetime(2020, 5, 15, 16, 45, 20, 0)],
    ['2020-05-15 16:45:20 Z', datetime(2020, 5, 15, 16, 45, 20, 0, tzinfo=UTC)],
    ['2020-05-15 16:45:20 Z', datetime(2020, 5, 15, 13, 45, 20, 0, tzinfo=GMT_MINUS_3)],
])
def test_datetime_rhs_should_equal_and_match_exact_datetime_lhs_computing_timezone_offsets(lhs, rhs):
    assert DateTime(rhs).equal(lhs) is True
    assert DateTime(rhs).match(lhs) is True


@parameterized.expand([
    ['2020-05-15T16:45:59', '2020-05-15T16:45:59.999999+00:00'],
    ['2020-05-15T16:45:59.999999+00:00', '2020-05-15T16:45:59'],
    ['2020-05-15T19:45:59.999999+03:00', '2020-05-15T16:45:59'],
    ['2020-05-15T02:00:00.123+00:00', datetime(2020, 5, 14, 23, 0, 0, 999, tzinfo=GMT_MINUS_3)]
])
def test_datetime_rhs_should_equal_and_match_datetime_lhs_ignoring_microseconds(lhs, rhs):
    assert DateTime(rhs).equal(lhs) is True
    assert DateTime(rhs).match(lhs) is True


@parameterized.expand([
    ['2020-05-15T16:45:59Z', '2020-05-15T16:45:59+03:00'],
    ['2020-05-15T16:00:00', '2020-05-15T16:45:59'],
    ['2020-05-14T16:45:59', '2020-05-15T16:45:59'],
    ['2020-05-15T04:00:00.123', datetime(2020, 5, 15, 1, 0, 0, 999)]
])
def test_datetime_rhs_should_not_equal_nor_match_different_datetime(lhs, rhs):
    assert DateTime(rhs).equal(lhs) is False
    assert DateTime(rhs).match(lhs) is False


@parameterized.expand([
    ['2020-05-15', '2020-05-15'],
    ['2020-05-15', date(2020, 5, 15)],

    ['2020-05-03', '2020-05-03'],
    ['2020-05-03', '2020-5-3'],
    ['2020-05-03', date(2020, 5, 3)],

    ['2020-5-3', '2020-5-3'],
    ['2020-5-3', '2020-05-03'],
    ['2020-5-3', date(2020, 5, 3)],

    [date(2020, 5, 15), date(2020, 5, 15)],
    [date(2020, 5, 15), '2020-05-15'],
    [date(2020, 5, 3), '2020-5-3']
])
def test_date_rhs_should_equal_and_match_exact_date_lhs(lhs, rhs):
    assert DateTime(rhs).equal(lhs) is True
    assert DateTime(rhs).match(lhs) is True


@parameterized.expand([
    ['2020-05-15', '2020-05-16'],
    ['2020-05-15', date(2020, 6, 15)],
    ['2020-05-14T15:45:13.123', date(2020, 6, 15)],
    [date(2020, 5, 15), date(2019, 5, 15)],
    [date(2020, 5, 15), '2021-05-15']
])
def test_date_rhs_should_not_equal_nor_match_different_date_lhs(lhs, rhs):
    assert DateTime(rhs).equal(lhs) is False
    assert DateTime(rhs).match(lhs) is False


@parameterized.expand([
    ['2020-05-03', '2020-5-3'],
    ['2020-5-3', '2020-05-03'],
    ['2020-12-15', date(2020, 12, 15)],
    ['2020-5-3', date(2020, 5, 3)],
    ['2020-05-03T15:45:59.123', '2020-05-03'],
    ['2020-05-03T15:45:59.123', '2020-5-3'],
    ['2020-05-15T15:45:59.123', date(2020, 5, 15)],
    ['2020-05-03T15:45:59.123', date(2020, 5, 3)],
    [dateutil.parser.parse('2020-05-15 23:59:59.999Z'), date(2020, 5, 15)],
])
def test_date_rhs_should_equal_and_match_datetime_lhs_if_date_component_is_the_same(lhs, rhs):
    assert DateTime(rhs).match(lhs) is True
    assert DateTime(rhs).equal(lhs) is True


@parameterized.expand([
    ['2020-05-15', '2020-05-15T15:45:59.123'],
    ['2020-05-15', datetime(2020, 5, 15, 15, 45, 59, 123)],
    ['2020-05-15', datetime(2020, 5, 15, 15, 45, 59, 123, tzinfo=UTC)],
    ['2020-05-15', datetime(2020, 5, 15, 15, 45, 59, 123, tzinfo=GMT_MINUS_3)],
    ['2020-05-15', '2020-05-15T15:45:59.123+01:00'],
    [date(2020, 5, 15), '2020-05-15T15:45:59.123+01:00'],
    ['2020-05-15', dateutil.parser.parse('2020-05-15T15:45:59.123-3:00')],
])
def test_datetime_rhs_should_equal_and_match_date_lhs_if_date_component_is_the_same(lhs, rhs):
    assert DateTime(rhs).equal(lhs) is True
    assert DateTime(rhs).match(lhs) is True


@parameterized.expand([
    ['2020-05-15T15:45:59.123', '2020-05-15'],
    [datetime(2020, 5, 15, 15, 45, 59, 123), '2020-05-15'],
    [datetime(2020, 5, 15, 15, 45, 59, 123, tzinfo=UTC), '2020-05-15'],
    [datetime(2020, 5, 15, 15, 45, 59, 123, tzinfo=GMT_MINUS_3), '2020-05-15'],
    ['2020-05-15T15:45:59.123+01:00', '2020-05-15'],
    ['2020-05-15T15:45:59.123+01:00', date(2020, 5, 15)],
    [dateutil.parser.parse('2020-05-15T15:45:59.123-3:00'), '2020-05-15'],
])
def test_date_rhs_should_equal_and_match_date_lhs_if_date_component_is_the_same(lhs, rhs):
    assert DateTime(rhs).equal(lhs) is True
    assert DateTime(rhs).match(lhs) is True


@parameterized.expand([
    ['2020-05-14', '2020-05-15T15:45:59.123'],
    ['2020-05-15', datetime(2019, 5, 15, 15, 45, 59, 123)],
    ['2020-04-15', dateutil.parser.parse('2020-05-15T15:45:59.123-3:00')],
])
def test_datetime_rhs_should_not_equal_nor_match_date_lhs_if_date_component_is_different(lhs, rhs):
    assert DateTime(rhs).match(lhs) is False
    assert DateTime(rhs).equal(lhs) is False


@parameterized.expand([
    ['2020-05-15T16:45:22', '2020-05-15T16:45:23'],
    ['2020-05-15T16:45:22Z', '2020-05-15T16:45:23+00:00'],
    [dateutil.parser.parse('2020-05-15T16:45:22'), '2020-05-15T16:45:23+00:00'],
    ['2020-05-15T16:00:00', '2020-05-15T13:45:23-03:00'],
    ['2020-05-14', '2020-05-15T16:45:23+00:00'],
    ['2020-05-15T16:45:23+00:00', '2021-05-14'],
    [date(2020, 5, 14), '2020-05-15T16:45:23+00:00'],
    ['2019-12-31', '2020-05-15T16:45:23+00:00'],
    ['2020-05-15T16:45:23+03:00', '2020-05-15T16:45:23+01:00'],
])
def test_datetime_lhs_should_be_less_than_datetime_rhs(lhs, rhs):
    assert DateTime(rhs).less_than(lhs) is True


@parameterized.expand([
    ['2020-05-15T16:45:22', '2020-05-15T16:45:22'],
    ['2020-05-15T16:45:25Z', '2020-05-15T16:45:23+00:00'],
    [dateutil.parser.parse('2020-05-15T16:45:22'), '2020-05-15T16:45:22+00:00'],
    ['2020-05-15T17:00:00', '2020-05-15T13:45:23-03:00'],
    ['2020-05-15', '2020-05-15T16:45:23+00:00'],
    ['2021-05-14', '2020-05-15T16:45:23+00:00'],
    [date(2020, 5, 15), '2020-05-15T16:45:23+00:00'],
    ['2020-05-15T16:45:23+00:00', '2019-12-31'],
    ['2020-05-03', '2020-05-02T23:59:59+03:00'],
])
def test_datetime_lhs_should_not_be_less_than_datetime_rhs(lhs, rhs):
    assert DateTime(rhs).less_than(lhs) is False


@parameterized.expand([
    ['2020-05-15T16:45:22', '2020-05-15T16:45:23'],
    ['2020-05-15T16:45:23', '2020-05-15T16:45:23'],
    ['2020-05-14', datetime(2020, 5, 15, 16, 45, 23)],
    ['2020-05-03', '2020-05-03T00:00:00+03:00'],
    ['2020-05-15T16:45:23+03:00', '2020-05-15T16:45:23+01:00'],
])
def test_datetime_lhs_should_be_less_than_or_equal_to_datetime_rhs(lhs, rhs):
    assert DateTime(rhs).less_than_or_equal(lhs) is True


@parameterized.expand([
    ['2020-05-16T16:45:22', '2020-05-15'],
    ['2020-05-16T16:45:22', date(2020, 5, 15)],
    ['2020-05-15T16:45:23+01:00', '2020-05-15T16:45:23+03:00'],
    ['2020-05-16', datetime(2020, 5, 15, 16, 45, 23)],
    ['2020-05-05', '2020-05-03T00:00:00+03:00'],
])
def test_datetime_lhs_should_not_be_less_than_or_equal_to_datetime_rhs(lhs, rhs):
    assert DateTime(rhs).less_than_or_equal(lhs) is False


@parameterized.expand([
    ['2020-05-15T16:45:23', '2020-05-15T16:45:22'],
    ['2020-05-15T16:45:23+00:00', '2020-05-15T16:45:22Z'],
    ['2020-05-15T16:45:23+00:00', dateutil.parser.parse('2020-05-15T16:45:22')],
    ['2020-05-15T13:45:23-03:00', '2020-05-15T16:00:00'],
    ['2020-05-15T16:45:23+00:00', '2020-05-14'],
    ['2021-05-14', '2020-05-15T16:45:23+00:00'],
    ['2020-05-15T16:45:23+00:00', date(2020, 5, 14)],
    ['2020-05-15T16:45:23+00:00', '2019-12-31'],
    ['2020-05-15T16:45:23+01:00', '2020-05-15T16:45:23+03:00'],
])
def test_datetime_lhs_should_be_greater_than_datetime_rhs(lhs, rhs):
    assert DateTime(rhs).greater_than(lhs) is True


@parameterized.expand([
    ['2020-05-15T16:45:22', '2020-05-15T16:45:22'],
    ['2020-05-15T16:45:23+00:00', '2020-05-15T16:45:25Z'],
    ['2020-05-15T16:45:22+00:00', dateutil.parser.parse('2020-05-15T16:45:22')],
    ['2020-05-15T13:45:23-03:00', '2020-05-15T17:00:00'],
    ['2020-05-15T16:45:23+00:00', '2020-05-15'],
    ['2020-05-15T16:45:23+00:00', '2021-05-14'],
    ['2020-05-15T16:45:23+00:00', date(2020, 5, 15)],
    ['2019-12-31', '2020-05-15T16:45:23+00:00'],
    ['2020-05-02T23:59:59+03:00', '2020-05-03'],
])
def test_datetime_lhs_should_not_be_greater_than_datetime_rhs(lhs, rhs):
    assert DateTime(rhs).greater_than(lhs) is False


@parameterized.expand([
    ['2020-05-15T16:45:23', '2020-05-15T16:45:22'],
    ['2020-05-15T16:45:23', '2020-05-15T16:45:23'],
    [datetime(2020, 5, 15, 16, 45, 23), '2020-05-14'],
    ['2020-05-03T00:00:00+03:00', '2020-05-03'],
    ['2020-05-15T16:45:23+01:00', '2020-05-15T16:45:23+03:00'],
])
def test_datetime_lhs_should_be_greater_than_or_equal_to_datetime_rhs(lhs, rhs):
    assert DateTime(rhs).greater_than_or_equal(lhs) is True


@parameterized.expand([
    ['2020-05-15', '2020-05-16T16:45:22'],
    [date(2020, 5, 15), '2020-05-16T16:45:22'],
    ['2020-05-15T16:45:23+03:00', '2020-05-15T16:45:23+01:00'],
    [datetime(2020, 5, 15, 16, 45, 23), '2020-05-16'],
    ['2020-05-03T00:00:00+03:00', '2020-05-05'],
])
def test_datetime_lhs_should_not_be_greater_than_or_equal_to_datetime_rhs(lhs, rhs):
    assert DateTime(rhs).greater_than_or_equal(lhs) is False
