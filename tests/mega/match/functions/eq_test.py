import decimal
from datetime import date

import dateutil.parser
import pytest
from parameterized import parameterized

from mega.match.functions import eq, Equal, match, one_of, gt, not_, and_, lt, gte
from mega.match.values import String, Boolean, DateTime, Null, Collection, Mapping, Number
from mega.match.values.value import LeftHandSideTypeError


@parameterized.expand([
    [Null, None],
    [String, 'foobar'],
    [Boolean, True],
    [Number, decimal.Decimal('123.99')],
    [DateTime, date(2020, 1, 15)],
    [Collection, ['one', 'two', 'three', match(r'four.*')]],
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
    [Collection((1, 2, 3, gt(4), 'five'))],
    [Mapping({
        'one': 1,
        'two': gte(2),
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


@parameterized.expand([
    [[1, 2, 3], (3, 2, 1)],
    [('a', 'b', 'b', 'c', 'd'), Collection(['c', 'b', 'd', 'a'])],
    [['Raphael', 'Leonardo', 'Donatello', 'Venus de Milo', 'Michelangelo'],
     ['Leonardo', 'Raphael', match(r'Donate.*'), 'Michelangelo', match(r'Venus [Dd]e Milo')]]
])
def test_equal_function_with_collection_rhs_evaluates_to_true(lhs, rhs):
    assert Equal(rhs).evaluate(lhs) is True


@parameterized.expand([
    [[1, 2, 3], (4, 3, 2, 1)],
    [('a', 'b', 'c', 'd'), Collection(['FOO', 'b', 'd', 'a'])],
    [['Raphael', 'Leonardo', 'DANTE', 'Venus de Milo', 'Michelangelo'],
     ['Leonardo', 'Raphael', match(r'Donate.*'), 'Michelangelo', match(r'Venus [Dd]e Milo')]]
])
def test_equal_function_with_collection_rhs_evaluates_to_false(lhs, rhs):
    assert Equal(rhs).evaluate(lhs) is False


@parameterized.expand([
    [
        {'one': 1, 'two': 'foo'},
        {'one': 1, 'two': 'foo'}
    ],
    [
        {
            'one': 1,
            'two': 42,
            'three': {
                'four': 'FooBar',
                'five': False
            },
            'six': 'Oatmeal'
        },
        {
            'one': 1,
            'two': and_(gt(30), lt(50)),
            'three': {
                'four': match(r'Foo.*')
            }
        }
    ],
    [
        {
            'type': 'user:notification:email',
            'notification_type': 'email',
            'ts': 1592163068,
            'user': {
                'id': 987650,
                'first_name': 'John',
                'email': 'johndoe_86@example.com',
                'template': 'password_changed'
            },
            'meta': {
                'generated_by': 'user_service',
                'ip_address': '177.182.215.204'
            }
        },
        dict(
            type=match(r'user:notification:(.*)'),
            notification_type=one_of('email', 'sms', 'push'),
            user=dict(
                id=gt(0),
                email=not_(match(r'test@(.*)')),
                template='password_changed'
            )
        )
    ]
])
def test_equal_function_with_mapping_rhs_evaluates_to_true(lhs, rhs):
    assert Equal(rhs).evaluate(lhs) is True


@parameterized.expand([
    [
        {'one': 1, 'two': 'foo'},
        {'one': 1, 'two': 'FooBar'}
    ],
    [
        {
            'one': 1,
            'two': 42,
            'three': {
                'four': 'FooBar',
                'five': True
            }
        },
        {
            'one': 1,
            'two': gt(100),
            'three': {
                'four': match(r'Foo.*'),
                'five': True
            }
        }
    ],
    [
        {
            'type': 'user:notification:email',
            'notification_type': 'email',
            'ts': 1592163068,
            'user': {
                'id': 987650,
                'first_name': 'John',
                'email': 'test@example.com',
                'template': 'password_changed'
            },
            'meta': {
                'generated_by': 'user_service',
                'ip_address': '177.182.215.204'
            }
        },
        dict(
            type=match(r'user:notification:(.*)'),
            notification_type=one_of('email', 'sms', 'push'),
            user=dict(
                id=gt(0),
                email=not_(match(r'test@(.*)')),
                template='password_changed'
            )
        )
    ]
])
def test_equal_function_with_mapping_rhs_evaluates_to_false(lhs, rhs):
    assert Equal(rhs).evaluate(lhs) is False


@parameterized.expand([
    ['foobar', Number(42)],
    [1999, DateTime('2020-12-15')],
    [42, 'Foo.*'],
    [[1, 2, 3], 'Bar'],
    [{'one': 1, 'two': 'foo'}, 42],
    [3, [1, 2, 3]]
])
def test_equal_function_fails_if_rhs_and_lhs_types_are_not_compatible(lhs, rhs):
    with pytest.raises(LeftHandSideTypeError):
        assert Equal(rhs).evaluate(lhs)
