from mega.match.functions.identity import identity
from mega.match.types import ValueType, RightHandSideType


def evaluate(lhs: ValueType, rhs: RightHandSideType) -> bool:
    return identity(rhs).evaluate(lhs)
