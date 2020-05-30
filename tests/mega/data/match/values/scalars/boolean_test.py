from datetime import date

import pytest
from parameterized import parameterized

from mega.data.match.values.scalars import Boolean
from mega.data.match.values.value import RightHandSideTypeError, LeftHandSideTypeError

TRUTHY_VALUES = [
    [True],
    [1],
    [42],
    ['true'],
    ['True'],
    ['abcdefgh12345'],
    ['0'],
    [[False]],
    [[1, 2, 3]],
    [{4, 5}],
    [date(2020, 5, 15)],
    [object()]
]

FALSEY_VALUES = [
    [False],
    [None],
    [0],
    [''],
    ['false'],
    ['False'],
    [[]],
    [()],
    [{}],
]


def test_boolean_true_rhs_should_equal_and_match_true_lhs():
    value = Boolean(True)
    assert value.equal(True) is True
    assert value.match(True) is True


def test_boolean_true_rhs_should_not_equal_neither_match_false_lhs():
    value = Boolean(True)
    assert value.equal(False) is False
    assert value.match(False) is False


def test_boolean_true_rhs_should_not_equal_neither_match_none_lhs():
    value = Boolean(True)
    assert value.equal(None) is False
    assert value.match(None) is False


def test_boolean_false_rhs_should_equal_and_match_false_lhs():
    value = Boolean(False)
    assert value.equal(False) is True
    assert value.match(False) is True


def test_boolean_false_rhs_should_not_equal_nor_match_true_lhs():
    value = Boolean(False)
    assert value.equal(True) is False
    assert value.match(True) is False


def test_boolean_false_rhs_should_not_equal_to_none_lhs():
    assert Boolean(False).equal(None) is False


def test_boolean_false_rhs_should_match_none_lhs():
    assert Boolean(False).match(None) is True


@parameterized.expand([
    ['True'],
    [0],
    [1],
    [[True]],
    [''],
    [[]],
    [{}],
    [object()]
])
def test_boolean_equal_should_not_accept_invalid_lhs(lhs):
    for rhs in (True, False):
        with pytest.raises(LeftHandSideTypeError) as e:
            Boolean(rhs).equal(lhs)
        assert '[Boolean.equal] Could not apply left-hand side <{}>'.format(type(lhs).__name__) in str(e.value)


@parameterized.expand(TRUTHY_VALUES)
def test_boolean_true_rhs_should_match_truthy_lhs_values(lhs):
    assert Boolean(True).match(lhs) is True


@parameterized.expand(FALSEY_VALUES)
def test_boolean_true_rhs_should_not_match_falsey_lhs_values(lhs):
    assert Boolean(True).match(lhs) is False


@parameterized.expand(FALSEY_VALUES)
def test_boolean_false_rhs_should_match_falsey_lhs_values(lhs):
    assert Boolean(False).match(lhs) is True


@parameterized.expand(TRUTHY_VALUES)
def test_boolean_false_rhs_should_not_match_truthy_lhs_values(lhs):
    assert Boolean(False).match(lhs) is False


@parameterized.expand([
    [None],
    ['True'],
    [0],
    [1],
    [[True]],
    [''],
    [[]],
    [{}],
    [object()]
])
def test_boolean_should_not_accept_invalid_rhs(rhs):
    with pytest.raises(RightHandSideTypeError) as e:
        Boolean(rhs)

    assert '[Boolean] Invalid right-hand side with type <{}>'.format(type(rhs).__name__) in str(e.value)
