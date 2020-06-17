import decimal
from datetime import date

import dateutil.parser
from parameterized import parameterized

from mega.match.functions import eq, Equal
from mega.match.values import String, Boolean, DateTime, Null, Collection, Mapping, Number


@parameterized.expand([
    [Null, None],
    [String, 'foobar'],
    [Boolean, True],
    [Number, decimal.Decimal('123.99')],
    [DateTime, date(2020, 1, 15)],
    [Collection, ['one', 'two', 'three']],
    [Mapping, {
        'a': 1,
        'b': {'c': 'd'}
    }],
])
def test_builds_equal_function_with_native_rhs(rhs_type, rhs_value):
    function = eq(rhs_value)
    assert type(function) == Equal
    assert type(function.rhs) == rhs_type
    assert function.rhs.rhs == rhs_value


@parameterized.expand([
    [Null()],
    [String(r'foobar.*')],
    [Boolean(True)],
    [Number(42)],
    [DateTime('2020-01-15T20:30:45')],
    [Collection((1, 2, 3, 4, 'five'))],
    [Mapping({
        'one': 1,
        'two': 2,
        'foo': {
            'a': 'b',
            'c': [3, 4, 5]
        }
    })],
])
def test_builds_equal_function_with_rhs_value(rhs):
    function = eq(rhs)
    assert type(function) == Equal
    assert function.rhs == rhs


@parameterized.expand([
    [None, None],
    [None, Null()],

    ['foo', 'foo'],
    ['FooBar', String('FooBar')],

    [True, True],
    [False, Boolean(False)],

    ['42', 42],
    ['0.99', Number(0.99)],

    ['2020-01-15T20:30:45.123', DateTime('2020-01-15T20:30:45')],
    ['2020-01-15T20:30:45', dateutil.parser.parse('2020-01-15T20:30:45.123')],
])
def test_equal_function_with_scalar_rhs_evaluates_to_true(lhs, rhs):
    assert Equal(rhs).evaluate(lhs) is True


@parameterized.expand([
    [None, 'foo'],
    [999, Null()],

    ['foo', 'bar'],
    ['BAR', String('barz')],

    [None, True],
    [True, Boolean(False)],

    ['42', 50],
    [0.99, Number('1000000')],

    ['2020-01-16T20:30:45.123', DateTime(date(2020, 1, 15))],
    ['2020-01-15T15:53:20', dateutil.parser.parse('2020-01-15T20:30:45.123')],
])
def test_equal_function_with_scalar_rhs_evaluates_to_false(lhs, rhs):
    assert Equal(rhs).evaluate(lhs) is False
