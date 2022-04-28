from sqs_mega_python_zwap.match.functions.value import ComparisonFunction
from sqs_mega_python_zwap.match.types import ValueType


class LessThan(ComparisonFunction):
    def evaluate(self, lhs: ValueType) -> bool:
        return self.rhs.less_than(lhs)


def lt(rhs) -> LessThan:
    return LessThan(rhs)
