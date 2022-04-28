from sqs_mega_python_zwap.match.functions.value import ComparisonFunction
from sqs_mega_python_zwap.match.types import ValueType


class LessThanOrEqual(ComparisonFunction):
    def evaluate(self, lhs: ValueType) -> bool:
        return self.rhs.less_than_or_equal(lhs)


def lte(rhs) -> LessThanOrEqual:
    return LessThanOrEqual(rhs)
