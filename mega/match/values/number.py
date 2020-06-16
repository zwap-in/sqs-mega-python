import decimal

from mega.match.values.base import ComparableValue
from mega.match.values.type import is_number, is_string, NumberType


class Number(ComparableValue):

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
