import decimal
from typing import Union

from sqs_mega_python_zwap.match.types import is_number, is_string, NumberType
from sqs_mega_python_zwap.match.values.value import ComparableValue


class Number(ComparableValue):

    def __init__(self, rhs: Union[NumberType, str]):
        super().__init__(rhs)

    @classmethod
    def accepts_rhs(cls, value):
        return is_number(value) or is_string(value)

    def _needs_casting(self, value, function_type=None):
        return is_string(value)

    def _cast(self, value, function_type=None, reference_value=None) -> NumberType:
        number_type = type(reference_value) if reference_value else decimal.Decimal
        return number_type(value)

    def _equal(self, lhs: NumberType):
        if lhs is None:
            return False

        return lhs == self.rhs

    def _less_than(self, lhs: NumberType):
        return lhs < self.rhs

    def _match(self, lhs: NumberType):
        return self._equal(lhs)
