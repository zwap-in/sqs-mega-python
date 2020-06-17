from typing import Union

from mega.match.functions.base import RightHandSideFunction
from mega.match.functions.identity import identity
from mega.match.values.base import RightHandSideValue
from mega.match.values.types import ValueType


def evaluate(lhs: ValueType, rhs: Union[ValueType, RightHandSideValue, RightHandSideFunction]) -> bool:
    return identity(rhs).evaluate(lhs)
