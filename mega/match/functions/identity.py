from typing import Union

from mega.match.functions import RightHandSideFunction, Equal
from mega.match.values.types import Value
from mega.match.values.value import RightHandSideValue


def identity(rhs: Union[Value, RightHandSideValue, RightHandSideFunction]) -> RightHandSideFunction:
    if isinstance(rhs, RightHandSideFunction):
        return rhs
    return Equal(rhs)