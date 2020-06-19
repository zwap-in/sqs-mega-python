from mega.match.functions.value import CollectionFunction
from mega.match.types import ValueType


class In(CollectionFunction):
    def evaluate(self, lhs: ValueType) -> bool:
        return self.rhs.contains(lhs)


def in_(rhs) -> In:
    return In(rhs)


def one_of(*rhs) -> In:
    return In(rhs)
