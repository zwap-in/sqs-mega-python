from typing import Union

from mega.match.functions.base import RightHandSideFunction
from mega.match.functions.identity import identity
from mega.match.values.types import ValueType
from mega.match.values.base import RightHandSideValue


def evaluate(rhs: Union[ValueType, RightHandSideValue, RightHandSideFunction], lhs: ValueType) -> bool:
    return identity(rhs).evaluate(lhs)
