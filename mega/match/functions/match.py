from mega.match.functions.base import ValueFunction
from mega.match.values.types import ValueType


class Match(ValueFunction):
    def evaluate(self, lhs: ValueType) -> bool:
        return self.rhs.match(lhs)


def match(rhs) -> Match:
    return Match(rhs)
