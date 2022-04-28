import pytest
from parameterized import parameterized

from sqs_mega_python_zwap.match.functions import eq, gt, not_, match, one_of, gte, and_, lt
from sqs_mega_python_zwap.match.values import Collection
from sqs_mega_python_zwap.match.values.value import RightHandSideTypeError, LeftHandSideTypeError


def test_empty_collection_rhs_is_equal_to_none_lhs():
    assert Collection([]).equal(None) is True


def test_empty_collection_rhs_matches_none_lhs():
    assert Collection([]).match(None) is True


def test_non_empty_collection_rhs_is_not_equal_to_none_lhs():
    assert Collection([1, 2, 3]).equal(None) is False


def test_non_empty_collection_rhs_does_not_match_none_lhs():
    assert Collection([1, 2, 3]).match(None) is False


@parameterized.expand([
    [[1], [1]],
    [['a'], ['a']],
    [[None], [None]],

    [[1, 2, 3], (1, 2, 3)],
    [[1, 2, 3], [1, 2, 3]],
    [[1, 2, 3], {1, 2, 3}],

    [[1, 2, 3], [3, 2, 1]],
    [[1, 2, 3], [eq(2), 3, 1]],
    [[1, 2, 3], [3, gt(1), not_(5)]],
    [['999', '998', '997'], [997, 998, 999]],

    [['foo', 'bar'], ['bar', one_of('foo', 'FOO', 'Foo', '_foo_')]],
    [['foo', 'bar', 'barz'], ['bar', match(r'bar[zZ]'), eq('foo')]],
    [['Raphael', 'Leonardo', 'Donatello', 'Venus de Milo', 'Michelangelo'],
     ['Leonardo', match(r'[Dd]onatel.*'), 'Raphael', 'Michelangelo', match(r'Venus [Dd]e Milo')]]
])
def test_collection_rhs_is_equal_to_collection_lhs(lhs, rhs):
    assert Collection(rhs).equal(lhs) is True


@parameterized.expand([
    [[], ['a', 'b', 'c']],
    [['five'], []],
    [[1], [2]],
    [['a'], ['D']],
    [[None], ['foo', 'bar']],
    [[1, 2, 3], (1, 2, 3, 4)],
    [[1, 2, 3], [3, 5, 1]],
    [['foo', 'bar'], ['bar']],
    [['foo', 'bar'], ['foo', 'BAR']],
    [['foo', 'bar'], ['bar', not_(one_of('foo', 'FOO', 'Foo', '_foo_'))]],
    [['Raphael', 'Leonardo', 'DANTE', 'Venus de Milo', 'Michelangelo'],
     ['Leonardo', match(r'[Dd]onatel.*'), 'Raphael', 'Michelangelo', match(r'Venus [Dd]e Milo')]],
])
def test_collection_rhs_is_not_equal_to_collection_lhs(lhs, rhs):
    assert Collection(rhs).equal(lhs) is False


@parameterized.expand([
    [[1, 2, 3], [1, 2, 3]],
    [['a', 'b', 'c'], ['c', 'b', 'a']],

    [[1, 2, 3, 4, 5], [3, 4, gte(5)]],
    [['bar', 'foo', 'barz'], ['foo', 'bar']],

    [['foo', 'bar'], ['bar', one_of('foo', 'FOO', 'Foo', '_foo_')]],
    [['foo', 'bar', 'barz'], ['bar', match(r'bar[zZ]'), eq('foo')]],
    [['Raphael', 'Leonardo', 'Donatello', 'Venus de Milo', 'Michelangelo'],
     [match(r'[Dd]onatel.*'), 'Michelangelo']]
])
def test_collection_rhs_matches_collection_lhs(lhs, rhs):
    assert Collection(rhs).match(lhs) is True


@parameterized.expand([
    [[1, 2, 3], [1, 2, 3, 4]],
    [['a', 'b', 'c'], ['c', 'd', 'a']],

    [[1, 2, 3, 4, 5], [3, 4, gt(5)]],
    [['bar', 'foo', 'barz'], ['foo', 'BAR']],

    [['foo'], ['bar', one_of('foo', 'FOO', 'Foo', '_foo_')]],
    [['bar', 'barz'], ['bar', match(r'bar[zZ]'), eq('foo')]],
    [['Raphael', 'Leonardo', 'Donatello', 'Venus de Milo', 'Michelangelo'], ['Dante', 'Michelangelo']]
])
def test_collection_rhs_does_not_match_collection_lhs(lhs, rhs):
    assert Collection(rhs).match(lhs) is False


@parameterized.expand([
    [1, [1, 2, 3]],
    ['bar', ['foo', match(r'[Bb]ar.*'), 'barz']],
])
def test_collection_rhs_matches_scalar_lhs(lhs, rhs):
    assert Collection(rhs).match(lhs) is True


@parameterized.expand([
    [None, [5]],
    [42, [1, 2, 3]],
    ['FooBar', ['foo', match(r'[Bb]ar.*'), 'barz']],
])
def test_collection_rhs_does_not_match_scalar_lhs(lhs, rhs):
    assert Collection(rhs).match(lhs) is False


@parameterized.expand([
    [None, [None]],
    ['a', ['b', 'c', 'a']],
    [1, [1, 2, gt(3)]],
    ['bar', ['foo', match(r'[Bb]ar.*'), 'barz']],
])
def test_collection_rhs_contains_scalar_lhs(lhs, rhs):
    assert Collection(rhs).contains(lhs) is True


@parameterized.expand([
    [None, [42]],
    ['foobar', ['b', 'c', 'a']],
    [1000000, [1, 2, and_(gte(3), lt(100))]],
    ['BAR', ['foo', match(r'[Bb]ar.*'), 'barz']],
])
def test_collection_rhs_does_not_contain_scalar_lhs(lhs, rhs):
    assert Collection(rhs).contains(lhs) is False


@parameterized.expand([
    [None],
    [''],
    ['2020-05-15#!'],
    [True],
    [1999],
    [object()],
    [{'foo': 'bar'}]
])
def test_collection_does_not_accept_invalid_rhs(rhs):
    with pytest.raises(RightHandSideTypeError) as e:
        Collection(rhs)

    assert '[Collection] Invalid right-hand side <{}>'.format(type(rhs).__name__) in str(e.value)


@parameterized.expand([
    [0, []],
    [1, [1]],
    ['a', ['a', 'b', 'c']],
    [1, ['a', 'b', 'c']],
    [True, [True, False]]
])
def test_collection_equal_does_not_accept_scalar_lhs(lhs, rhs):
    with pytest.raises(LeftHandSideTypeError) as e:
        Collection(rhs).equal(lhs)

    assert '[Collection.equal] Could not apply left-hand side <{}>'.format(type(lhs).__name__) in str(e.value)


@parameterized.expand([
    [object(), [1, 2, 3]],
    [{'foo': 'bar'}, ['foo', 'bar']],
])
def test_collection_equal_does_not_accept_invalid_lhs(lhs, rhs):
    with pytest.raises(LeftHandSideTypeError) as e:
        Collection(rhs).equal(lhs)

    assert '[Collection.equal] Could not apply left-hand side <{}>'.format(type(lhs).__name__) in str(e.value)


@parameterized.expand([
    [[1, 'a', 3], ['a', lt(5), 'b']],
    [[1, 2, 3], ['a', 'b']],
    [[True], ['a', 'b', 'c']],
    [['Raphael', 'Leonardo', 'Donatello', 42, 'Venus de Milo', 'Michelangelo'],
     ['Leonardo', match(r'[Dd]onatel.*'), 'Raphael', 'Michelangelo', match(r'Venus [Dd]e Milo')]]
])
def test_collection_equal_does_not_accept_collection_lhs_with_incompatible_types(lhs, rhs):
    with pytest.raises(LeftHandSideTypeError) as e:
        Collection(rhs).equal(lhs)

    assert (
            '[Collection.equal] Could not apply left-hand side <list> ([…]) to right-hand side <list> ([…]). '
            'Collections have incompatible types.' in str(e.value)
    )


@parameterized.expand([
    [object(), [1, 2, 3]],
    [{'foo': 'bar'}, ['foo', 'bar']],
])
def test_collection_match_does_not_accept_invalid_lhs(lhs, rhs):
    with pytest.raises(LeftHandSideTypeError) as e:
        Collection(rhs).match(lhs)

    assert '[Collection.match] Could not apply left-hand side <{}>'.format(type(lhs).__name__) in str(e.value)


@parameterized.expand([
    [[1, 'a', 3], ['a', lt(5), 'b']],
    [['Raphael', 'Leonardo', 'Donatello', 42, 'Venus de Milo', 'Michelangelo'],
     ['Leonardo', match(r'[Dd]onatel.*'), 'Raphael', 'Michelangelo', match(r'Venus [Dd]e Milo')]]
])
def test_collection_match_does_not_accept_collection_lhs_with_incompatible_types(lhs, rhs):
    with pytest.raises(LeftHandSideTypeError) as e:
        Collection(rhs).match(lhs)

    assert '[Collection.match] Could not apply left-hand side' in str(e.value)
    assert 'Collections have incompatible types.' in str(e.value)


@parameterized.expand([
    [5, ['a', eq(5), 'b']],
    [True, ['a', 'b']],
    ['one', [1, 2, 3]]
])
def test_collection_match_does_not_accept_scalar_lhs_with_incompatible_types(lhs, rhs):
    with pytest.raises(LeftHandSideTypeError) as e:
        Collection(rhs).match(lhs)

    assert '[Collection.match] Could not apply left-hand side' in str(e.value)
    assert 'Left-hand side is not compatible with collection type.' in str(e.value)


@parameterized.expand([
    ['foo', [1, 2, 3]],
    [42, ['a', match('[Bb].*'), 'c']],
    [['foo'], ['foo', 'bar']],
    [[1, 2, 3], [1, 2, 3, 4, 5, 6]],
    [{'foo': 'bar'}, ['foo', 'bar']],
])
def test_collection_contains_does_not_accept_invalid_lhs(lhs, rhs):
    with pytest.raises(LeftHandSideTypeError) as e:
        Collection(rhs).contains(lhs)

    assert '[Collection.contains] Could not apply left-hand side <{}>'.format(type(lhs).__name__) in str(e.value)


@parameterized.expand([
    [5, ['a', eq(5), 'b']],
    [True, ['a', 'b']],
    ['one', [1, 2, 3]]
])
def test_collection_contains_does_not_accept_scalar_lhs_with_incompatible_types(lhs, rhs):
    with pytest.raises(LeftHandSideTypeError) as e:
        Collection(rhs).contains(lhs)

    assert '[Collection.contains] Could not apply left-hand side' in str(e.value)
    assert 'Left-hand side is not compatible with collection type.' in str(e.value)
