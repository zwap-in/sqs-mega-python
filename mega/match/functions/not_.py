from typing import Union

from mega.match.functions.base import RightHandSideFunction, HigherOrderFunction
from mega.match.functions.identity import identity
from mega.match.values.type import Value
from mega.match.values.base import RightHandSideValue


class Not(HigherOrderFunction):

    def __init__(self, rhs: Union[Value, RightHandSideValue, RightHandSideFunction]):
        super().__init__(identity(rhs))

    def evaluate(self, lhs: Value) -> bool:
        return not self.rhs.evaluate(lhs)


def not_(rhs) -> Not:
    return Not(rhs)


def neq(rhs) -> Not:
    return Not(rhs)
