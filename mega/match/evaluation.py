from typing import Union

from mega.match.functions.base import RightHandSideFunction
from mega.match.functions.identity import identity
from mega.match.values.types import Value
from mega.match.values.value import RightHandSideValue


def evaluate(rhs: Union[Value, RightHandSideValue, RightHandSideFunction], lhs: Value) -> bool:
    return identity(rhs).evaluate(lhs)
