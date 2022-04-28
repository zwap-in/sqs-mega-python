from sqs_mega_python_zwap.match.functions import CombinedFunction
from sqs_mega_python_zwap.match.types import ValueType


class Or(CombinedFunction):
    def evaluate(self, lhs: ValueType) -> bool:
        return any(
            i.evaluate(lhs)
            for i in self.rhs
        )


def or_(*rhs) -> Or:
    return Or(rhs)
