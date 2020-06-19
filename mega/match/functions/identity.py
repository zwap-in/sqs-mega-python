from mega.match.functions.eq import Equal
from mega.match.functions.lambda_ import Lambda
from mega.match.types import is_function, RightHandSideFunction, RightHandSideType


def identity(rhs: RightHandSideType) -> RightHandSideFunction:
    if isinstance(rhs, RightHandSideFunction):
        return rhs

    if is_function(rhs):
        return Lambda(rhs)

    return Equal(rhs)
