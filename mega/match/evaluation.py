from typing import Union

from mega.match.functions import RightHandSideFunction, identity
from mega.match.values.value import RightHandSideValue
from mega.match.values.types import Value


def evaluate(rhs: Union[Value, RightHandSideValue, RightHandSideFunction], lhs: Value) -> bool:
    return identity(rhs).evaluate(lhs)
