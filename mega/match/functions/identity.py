from typing import Union

from mega.match.functions import RightHandSideFunction, Equal
from mega.match.values.base import RightHandSideValue
from mega.match.values.type import Value


def identity(rhs: Union[Value, RightHandSideValue, RightHandSideFunction]) -> RightHandSideFunction:
    if isinstance(rhs, RightHandSideFunction):
        return rhs
    return Equal(rhs)
