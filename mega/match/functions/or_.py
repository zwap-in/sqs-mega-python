from mega.match.functions.base import CombinedFunction
from mega.match.values.types import Value


class Or(CombinedFunction):
    def evaluate(self, lhs: Value) -> bool:
        return any(
            i.evaluate(lhs)
            for i in self.rhs
        )


def or_(*rhs) -> Or:
    return Or(rhs)
