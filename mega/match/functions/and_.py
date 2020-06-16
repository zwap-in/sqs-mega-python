from mega.match.functions.base import CombinedFunction
from mega.match.values.type import Value


class And(CombinedFunction):
    def evaluate(self, lhs: Value) -> bool:
        return all(
            i.evaluate(lhs)
            for i in self.rhs
        )


def and_(*rhs) -> And:
    return And(rhs)
