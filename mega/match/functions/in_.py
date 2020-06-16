from mega.match.functions.base import CollectionFunction
from mega.match.values.types import Value


class In(CollectionFunction):
    def evaluate(self, lhs: Value) -> bool:
        return self.rhs.contains(lhs)


def in_(rhs) -> In:
    return In(rhs)


def one_of(*rhs) -> In:
    return In(rhs)
