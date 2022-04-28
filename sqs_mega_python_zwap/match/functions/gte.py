from sqs_mega_python_zwap.match.functions.value import ComparisonFunction
from sqs_mega_python_zwap.match.types import ValueType


class GreaterThanOrEqual(ComparisonFunction):
    def evaluate(self, lhs: ValueType) -> bool:
        return self.rhs.greater_than_or_equal(lhs)


def gte(rhs) -> GreaterThanOrEqual:
    return GreaterThanOrEqual(rhs)
