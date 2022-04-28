from sqs_mega_python_zwap.match.functions.identity import identity
from sqs_mega_python_zwap.match.types import ValueType, RightHandSideType


def evaluate(lhs: ValueType, rhs: RightHandSideType) -> bool:
    return identity(rhs).evaluate(lhs)
