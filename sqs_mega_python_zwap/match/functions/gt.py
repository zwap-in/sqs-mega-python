from sqs_mega_python_zwap.match.functions.value import ComparisonFunction
from sqs_mega_python_zwap.match.types import ValueType


class GreaterThan(ComparisonFunction):
    def evaluate(self, lhs: ValueType) -> bool:
        return self.rhs.greater_than(lhs)


def gt(rhs) -> GreaterThan:
    return GreaterThan(rhs)
