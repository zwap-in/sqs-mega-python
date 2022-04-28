from datetime import date

from parameterized import parameterized

from sqs_mega_python_zwap.match.values.null import Null

FALSEY_VALUES = [
    [[]],
    [()],
    [{}],
    [''],
    [False],
    [0]
]

TRUTHY_VALUES = [
    [True],
    [1],
    [42],
    ['true'],
    ['True'],
    ['false'],
    ['False'],
    ['abcdefgh12345'],
    ['0'],
    [[False]],
    [[1, 2, 3]],
    [{4, 5}],
    [date(2020, 5, 15)],
    [object()]
]


def test_null_rhs_should_equal_to_none_lhs():
    assert Null().equal(None) is True


def test_null_rhs_should_match_none_lhs():
    assert Null().match(None) is True


@parameterized.expand(FALSEY_VALUES)
def test_null_should_not_equal_to_falsey_lhs(lhs):
    assert Null().equal(lhs) is False


@parameterized.expand(TRUTHY_VALUES)
def test_null_should_not_equal_to_truthy_lhs(lhs):
    assert Null().equal(lhs) is False


@parameterized.expand(FALSEY_VALUES)
def test_null_should_match_falsey_lhs(lhs):
    assert Null().match(lhs) is True


@parameterized.expand(TRUTHY_VALUES)
def test_null_should_not_match_truthy_lhs(lhs):
    assert Null().match(lhs) is False
