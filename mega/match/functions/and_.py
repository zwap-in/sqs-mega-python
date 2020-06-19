from mega.match.functions.higher_order import CombinedFunction
from mega.match.types import ValueType


class And(CombinedFunction):
    def evaluate(self, lhs: ValueType) -> bool:
        return all(
            i.evaluate(lhs)
            for i in self.rhs
        )


def and_(*rhs) -> And:
    return And(rhs)
