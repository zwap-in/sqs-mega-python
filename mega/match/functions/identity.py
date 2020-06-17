from typing import Union

from mega.match.functions.base import RightHandSideFunction
from mega.match.functions.eq import Equal
from mega.match.values.base import RightHandSideValue
from mega.match.values.types import ValueType


def identity(rhs: Union[ValueType, RightHandSideValue, RightHandSideFunction]) -> RightHandSideFunction:
    if isinstance(rhs, RightHandSideFunction):
        return rhs
    return Equal(rhs)
