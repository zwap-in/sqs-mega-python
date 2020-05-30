import pytest
from parameterized import parameterized

from mega.data.match.values.scalars import String
from mega.data.match.values.value import RightHandSideTypeError, LeftHandSideTypeError


@parameterized.expand([
    [''],
    [' '],
    ['123'],
    ['foo.bar'],
    ['FooBar'],
    ['João César'],
])
def test_string_rhs_should_be_equal_to_string_lhs(value):
    assert String(value).equal(value) is True


@parameterized.expand([
    ['foo', None],
    ['foo', ''],
    ['foo.bar', 'Foo.bar'],
    ['FooBar', '123'],
    ['João César', 'Joao Cesar']
])
def test_string_rhs_should_not_be_equal_to_string_lhs(rhs, lhs):
    assert String(rhs).equal(lhs) is False


@parameterized.expand([
    ['foobar123', 'foobar123'],
    [r'foo.(.*)', 'foo.bar'],
    [r'foo:(.*)', 'foo:bar:boo'],
    [r'[Ff]oo[Bb]ar', 'FooBar'],
    [r'[Ff]oo[Bb]ar', 'foobar'],
    [r'João Vinícius(.*)', 'João Vinícius Pereira'],
    ['Wayne', 'Wayne Enterprises']
])
def test_string_rhs_should_match_string_lhs(rhs, lhs):
    assert String(rhs).match(lhs) is True


@parameterized.expand([
    ['foobar123', 'FooBar123'],
    [r'foo\.(.*)', 'foo:bar'],
    [r'foo:(.*)', 'foo.bar.boo'],
    [r'[Ff]oo[Bb]ar', 'ZooBar'],
    [r'[Ff]oo[Bb]ar', 'zoobar'],
    [r'^Wayne', 'Bruce Wayne'],
])
def test_string_rhs_should_not_match_string_lhs(rhs, lhs):
    assert String(rhs).match(lhs) is False


@parameterized.expand([
    [None],
    ['']
])
def test_empty_string_rhs_should_be_equal_to_string_lhs(lhs):
    assert String('').equal(lhs) is True


@parameterized.expand([
    [None],
    ['']
])
def test_empty_string_rhs_should_match_string_lhs(lhs):
    assert String('').match(lhs) is True


@parameterized.expand([
    [None],
    [True],
    [1],
    [['asdf']],
    [object()]
])
def test_string_should_not_accept_invalid_rhs(rhs):
    with pytest.raises(RightHandSideTypeError) as e:
        String(rhs)

    assert '[String] Invalid right-hand side with type <{}>'.format(type(rhs).__name__) in str(e.value)


@parameterized.expand([
    [True],
    [1],
    [['asdf']],
    [{'abc'}],
    [object()]
])
def test_string_equal_should_not_accept_invalid_lhs(lhs):
    with pytest.raises(LeftHandSideTypeError) as e:
        String('foo.bar').equal(lhs)

    assert '[String.equal] Could not apply left-hand side <{}>'.format(type(lhs).__name__) in str(e.value)


@parameterized.expand([
    [True],
    [1],
    [['asdf']],
    [{'abc'}],
    [object()]
])
def test_string_match_should_not_accept_invalid_lhs(lhs):
    with pytest.raises(LeftHandSideTypeError) as e:
        String('foo.bar').match(lhs)

    assert '[String.match] Could not apply left-hand side <{}>'.format(type(lhs).__name__) in str(e.value)
