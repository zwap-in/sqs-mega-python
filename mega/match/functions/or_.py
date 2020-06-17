from mega.match.functions.base import CombinedFunction
from mega.match.values.types import ValueType


class Or(CombinedFunction):
    def evaluate(self, lhs: ValueType) -> bool:
        return any(
            i.evaluate(lhs)
            for i in self.rhs
        )


def or_(*rhs) -> Or:
    return Or(rhs)
