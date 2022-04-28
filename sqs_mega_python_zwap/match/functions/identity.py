from sqs_mega_python_zwap.match.functions.eq import Equal
from sqs_mega_python_zwap.match.functions.lambda_ import Lambda
from sqs_mega_python_zwap.match.types import is_function, RightHandSideFunction, RightHandSideType


def identity(rhs: RightHandSideType) -> RightHandSideFunction:
    if isinstance(rhs, RightHandSideFunction):
        return rhs

    if is_function(rhs):
        return Lambda(rhs)

    return Equal(rhs)
