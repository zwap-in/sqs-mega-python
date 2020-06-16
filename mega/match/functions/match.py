from mega.match.functions.base import ValueFunction
from mega.match.values.types import Value


class Match(ValueFunction):
    def evaluate(self, lhs: Value) -> bool:
        return self.rhs.match(lhs)


def match(rhs) -> Match:
    return Match(rhs)
