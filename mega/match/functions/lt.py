from mega.match.functions.base import ComparisonFunction
from mega.match.values.type import Value


class LessThan(ComparisonFunction):
    def evaluate(self, lhs: Value) -> bool:
        return self.rhs.less_than(lhs)


def lt(rhs) -> LessThan:
    return LessThan(rhs)
