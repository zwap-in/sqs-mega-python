from mega.match.functions.base import ComparisonFunction
from mega.match.values.types import Value


class GreaterThanOrEqual(ComparisonFunction):
    def evaluate(self, lhs: Value) -> bool:
        return self.rhs.greater_than_or_equal(lhs)


def gte(rhs) -> GreaterThanOrEqual:
    return GreaterThanOrEqual(rhs)
