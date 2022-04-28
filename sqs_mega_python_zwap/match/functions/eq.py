from sqs_mega_python_zwap.match.functions.value import ValueFunction
from sqs_mega_python_zwap.match.types import ValueType


class Equal(ValueFunction):
    def evaluate(self, lhs: ValueType) -> bool:
        return self.rhs.equal(lhs)


def eq(rhs) -> Equal:
    return Equal(rhs)
