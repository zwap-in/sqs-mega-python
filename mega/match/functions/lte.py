from mega.match.functions.base import ComparisonFunction
from mega.match.values.types import Value


class LessThanOrEqual(ComparisonFunction):
    def evaluate(self, lhs: Value) -> bool:
        return self.rhs.less_than_or_equal(lhs)


def lte(rhs) -> LessThanOrEqual:
    return LessThanOrEqual(rhs)
