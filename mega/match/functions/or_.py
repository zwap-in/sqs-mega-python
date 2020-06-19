from mega.match.functions import CombinedFunction
from mega.match.types import ValueType


class Or(CombinedFunction):
    def evaluate(self, lhs: ValueType) -> bool:
        return any(
            i.evaluate(lhs)
            for i in self.rhs
        )


def or_(*rhs) -> Or:
    return Or(rhs)
