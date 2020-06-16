from mega.match.functions.base import ValueFunction
from mega.match.values.type import Value


class Equal(ValueFunction):
    def evaluate(self, lhs: Value) -> bool:
        return self.rhs.equal(lhs)


def eq(rhs) -> Equal:
    return Equal(rhs)
