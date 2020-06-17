from mega.match.functions.base import ValueFunction
from mega.match.values.types import ValueType


class Equal(ValueFunction):
    def evaluate(self, lhs: ValueType) -> bool:
        return self.rhs.equal(lhs)


def eq(rhs) -> Equal:
    return Equal(rhs)
