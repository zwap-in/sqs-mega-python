import sys
from decimal import Decimal

import pytest
from parameterized import parameterized

from mega.match.values.number import Number
from mega.match.values.value import LeftHandSideTypeError, RightHandSideTypeError


@parameterized.expand([
    [0, 0],
    [1, 1],
    [1, 1.0],
    [1, Decimal('1.0')],
    [1.0, 1],
    [1.0, 1.0],
    [1.0, Decimal('1.0')],
    [Decimal('1.0'), 1],
    [Decimal('1.0'), 1.0],
    [Decimal('1.0'), Decimal('1.0')],
    [0.12345678, 0.12345678],
    [Decimal('0.12345678'), Decimal('0.12345678')]
])
def test_number_rhs_should_equal_and_match_number_lhs(lhs, rhs):
    assert Number(rhs).equal(lhs) is True
    assert Number(rhs).match(lhs) is True


@parameterized.expand([
    [0, 1],
    [1, 0],
    [1, 1.01],
    [1.01, 1],
    [1, Decimal('1.01')],
    [Decimal('1.01'), 1.2345],
    [0.12345, 0],
    [1, sys.maxsize],
    [sys.maxsize, 1]
])
def test_number_rhs_should_not_equal_nor_match_number_lhs(lhs, rhs):
    assert Number(rhs).equal(lhs) is False
    assert Number(rhs).match(lhs) is False


@parameterized.expand([
    ['0', 0],
    ['1', 1],
    ['1', 1.0],
    ['1', Decimal('1.0')],
    ['1.0', 1.0],
    ['1.0', Decimal('1.0')],
    ['0.12345678', 0.12345678],
    ['0.12345678', Decimal('0.12345678')]
])
def test_number_rhs_should_equal_to_equivalent_string_lhs_representing_number(lhs, rhs):
    assert Number(rhs).equal(lhs) is True
    assert Number(rhs).match(lhs) is True


@parameterized.expand([
    ['0', 1],
    ['1', 0],
    ['1', 1.01],
    ['1', Decimal('1.01')],
    ['1.01', 1.2345],
    ['1', sys.maxsize],
    [str(sys.maxsize), 1]
])
def test_number_rhs_should_not_equal_nor_match_inequivalent_string_lhs_representing_number(lhs, rhs):
    assert Number(rhs).equal(lhs) is False
    assert Number(rhs).match(lhs) is False


@parameterized.expand([
    [0],
    [123]
])
def test_number_rhs_should_not_equal_to_none_lhs(rhs):
    assert Number(rhs).equal(None) is False


@parameterized.expand([
    ['1.0', 1],
    ['foobar', 1],
    ['!@#$', 1.0],
    ['', 0],
])
def test_number_equal_should_not_accept_string_lhs_that_does_not_represent_a_valid_number_with_rhs_type(lhs, rhs):
    with pytest.raises(LeftHandSideTypeError) as e:
        Number(rhs).equal(lhs)

    assert '[Number.equal] Could not apply left-hand side <{}>'.format(type(lhs).__name__) in str(e.value)


@parameterized.expand([
    ['1.0', 1],
    ['foobar', 1],
    ['!@#$', 1.0],
    ['', 0],
])
def test_number_match_should_not_accept_string_lhs_that_does_not_represent_a_valid_number_with_rhs_type(lhs, rhs):
    with pytest.raises(LeftHandSideTypeError) as e:
        Number(rhs).match(lhs)

    assert '[Number.match] Could not apply left-hand side <{}>'.format(type(lhs).__name__) in str(e.value)


@parameterized.expand([
    [None],
    [True],
    [False],
    [''],
    ['foo'],
    ['123#foo.bar'],
    [[]],
    [(1, 2, 3)],
    [{4, 5}],
    [object()]
])
def test_number_should_not_accept_invalid_rhs(rhs):
    with pytest.raises(RightHandSideTypeError) as e:
        Number(rhs)

    assert '[Number] Invalid right-hand side <{}>'.format(type(rhs).__name__) in str(e.value)


@parameterized.expand([
    ['0'],
    ['1'],
    ['1000000000'],
    ['123.456'],
    ['0.999']
])
def test_number_should_accept_string_rhs_representing_number(rhs):
    value = Number(rhs)
    assert type(value.rhs) is Decimal
    assert value.rhs == Decimal(rhs)


@parameterized.expand([
    [1, 0],
    [1.01, 1],
    [5, Decimal('4.9999999999')],
    ['5', 4],
    ['5.01', 5.009]
])
def test_number_greater_than(lhs, rhs):
    assert Number(rhs).greater_than(lhs) is True


@parameterized.expand([
    [0, 0],
    ['1.01', 1.01],
    [0, 1],
    [1, 1.01],
    [Decimal('4.9999999999'), 5],
    ['4', 5],
    ['5.009', 5.01]
])
def test_number_not_greater_than(lhs, rhs):
    assert Number(rhs).greater_than(lhs) is False


@parameterized.expand([
    [None],
    ['foo.bar'],
    [True],
    [[1]]
])
def test_number_greater_than_should_not_accept_invalid_lhs(lhs):
    with pytest.raises(LeftHandSideTypeError) as e:
        Number(5).greater_than(lhs)
    assert e.value.lhs == lhs
    assert e.value.function_type == Number.FunctionType.GREATER_THAN


@parameterized.expand([
    [0, 0],
    ['1.01', 1.01],
    ['0.12345678', Decimal('0.12345678')],
    [1, 0],
    [1.01, 1],
    [5, Decimal('4.9999999999')],
    ['5', 4],
    ['5.01', 5.009]
])
def test_number_greater_than_or_equal(lhs, rhs):
    assert Number(rhs).greater_than_or_equal(lhs) is True


@parameterized.expand([
    [0, 1],
    [1, 1.01],
    [Decimal('4.9999999999'), 5],
    ['4', 5],
    ['5.009', 5.01]
])
def test_number_not_greater_than_or_equal(lhs, rhs):
    assert Number(rhs).greater_than_or_equal(lhs) is False


@parameterized.expand([
    [None],
    ['foo.bar'],
    [True],
    [[1]]
])
def test_number_greater_than_or_equal_should_not_accept_invalid_lhs(lhs):
    with pytest.raises(LeftHandSideTypeError) as e:
        Number(5).greater_than_or_equal(lhs)
    assert e.value.lhs == lhs
    assert e.value.function_type == Number.FunctionType.GREATER_THAN_OR_EQUAL


@parameterized.expand([
    [0, 1],
    [1, 1.01],
    [Decimal('4.9999999999'), 5],
    ['4', 5],
    ['5.009', 5.01]
])
def test_number_less_than(lhs, rhs):
    assert Number(rhs).less_than(lhs) is True


@parameterized.expand([
    [0, 0],
    ['1.01', 1.01],
    [1, 0],
    [1.01, 1],
    [5, Decimal('4.9999999999')],
    ['5', 4],
    ['5.01', 5.009]
])
def test_number_not_less_than(lhs, rhs):
    assert Number(rhs).less_than(lhs) is False


@parameterized.expand([
    [None],
    ['foo.bar'],
    [True],
    [[1]]
])
def test_number_less_than_should_not_accept_invalid_lhs(lhs):
    with pytest.raises(LeftHandSideTypeError) as e:
        Number(5).less_than(lhs)
    assert e.value.lhs == lhs
    assert e.value.function_type == Number.FunctionType.LESS_THAN


@parameterized.expand([
    [0, 0],
    ['1.01', 1.01],
    ['0.12345678', Decimal('0.12345678')],
    [0, 1],
    [1, 1.01],
    [Decimal('4.9999999999'), 5],
    ['4', 5],
    ['5.009', 5.01]
])
def test_number_less_than_or_equal(lhs, rhs):
    assert Number(rhs).less_than_or_equal(lhs) is True


@parameterized.expand([
    [1, 0],
    [1.01, 1],
    [5, Decimal('4.9999999999')],
    ['5', 4],
    ['5.01', 5.009]
])
def test_number_not_less_than_or_equal(lhs, rhs):
    assert Number(rhs).less_than_or_equal(lhs) is False


@parameterized.expand([
    [None],
    ['foo.bar'],
    [True],
    [[1]]
])
def test_number_less_than_or_equal_should_not_accept_invalid_lhs(lhs):
    with pytest.raises(LeftHandSideTypeError) as e:
        Number(5).less_than_or_equal(lhs)
    assert e.value.lhs == lhs
    assert e.value.function_type == Number.FunctionType.LESS_THAN_OR_EQUAL
