from mega.match.functions.value import ValueFunction
from mega.match.types import ValueType


class Equal(ValueFunction):
    def evaluate(self, lhs: ValueType) -> bool:
        return self.rhs.equal(lhs)


def eq(rhs) -> Equal:
    return Equal(rhs)
