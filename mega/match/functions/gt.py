from mega.match.functions.base import ComparisonFunction
from mega.match.values.types import Value


class GreaterThan(ComparisonFunction):
    def evaluate(self, lhs: Value) -> bool:
        return self.rhs.greater_than(lhs)


def gt(rhs) -> GreaterThan:
    return GreaterThan(rhs)
