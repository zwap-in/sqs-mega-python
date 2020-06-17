from mega.match.functions.base import ComparisonFunction
from mega.match.values.types import ValueType


class GreaterThanOrEqual(ComparisonFunction):
    def evaluate(self, lhs: ValueType) -> bool:
        return self.rhs.greater_than_or_equal(lhs)


def gte(rhs) -> GreaterThanOrEqual:
    return GreaterThanOrEqual(rhs)
