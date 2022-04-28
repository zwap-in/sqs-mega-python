from sqs_mega_python_zwap.match.types import ValueType, FunctionType, is_function, RightHandSideFunction


class Lambda(RightHandSideFunction):
    def __init__(self, rhs: FunctionType):
        if not is_function(rhs):
            raise TypeError('Right-hand side is not a user-defined function: {}'.format(type(rhs).__name__))
        self.rhs = rhs

    def evaluate(self, lhs: ValueType) -> bool:
        return bool(self.rhs(lhs))
