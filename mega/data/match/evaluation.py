from typing import Union

from mega.data.match.functions import RightHandSideFunction, identity
from mega.data.match.values.value import RightHandSideValue
from mega.data.match.values.types import Value


def evaluate(rhs: Union[Value, RightHandSideValue, RightHandSideFunction], lhs: Value) -> bool:
    return identity(rhs).evaluate(lhs)
