from mega.match.functions.higher_order import HigherOrderFunction
from mega.match.functions.identity import identity
from mega.match.types import ValueType, RightHandSideType


class Not(HigherOrderFunction):

    def __init__(self, rhs: RightHandSideType):
        super().__init__(identity(rhs))

    def evaluate(self, lhs: ValueType) -> bool:
        return not self.rhs.evaluate(lhs)


def not_(rhs) -> Not:
    return Not(rhs)


def neq(rhs) -> Not:
    return Not(rhs)
