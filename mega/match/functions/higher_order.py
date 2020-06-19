from abc import ABC
from typing import Collection

from mega.match.functions.lambda_ import Lambda
from mega.match.types import RightHandSideFunction, is_function, RightHandSideFunctionType


def function(rhs):
    if isinstance(rhs, RightHandSideFunction):
        return rhs

    if is_function(rhs):
        return Lambda(rhs)

    raise TypeError


class CombinedFunction(RightHandSideFunction, ABC):
    def __init__(self, rhs: Collection[RightHandSideFunctionType]):
        if len(rhs) < 2:
            raise ValueError

        self.rhs = [
            function(i)
            for i in rhs
        ]


class HigherOrderFunction(RightHandSideFunction, ABC):
    def __init__(self, rhs: RightHandSideFunctionType):
        self.rhs = function(rhs)
