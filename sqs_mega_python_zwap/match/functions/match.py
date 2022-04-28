from sqs_mega_python_zwap.match.functions.value import ValueFunction
from sqs_mega_python_zwap.match.types import ValueType


class Match(ValueFunction):
    def evaluate(self, lhs: ValueType) -> bool:
        return self.rhs.match(lhs)


def match(rhs) -> Match:
    return Match(rhs)
