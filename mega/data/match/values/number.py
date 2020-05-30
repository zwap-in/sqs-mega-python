import decimal

from mega.data.match.values.types import is_number, is_string, NumberType
from mega.data.match.values.value import ComparableValue


class Number(ComparableValue):

    @classmethod
    def accepts_rhs(cls, value):
        return is_number(value) or is_string(value)

    def _needs_casting(self, value, function_type=None):
        return is_string(value)

    def _cast(self, value, reference_type=None) -> NumberType:
        number_type = reference_type if reference_type else decimal.Decimal
        return number_type(value)

    def _equal(self, lhs: NumberType):
        if lhs is None:
            return False

        return lhs == self.rhs

    def _less_than(self, lhs: NumberType):
        return lhs < self.rhs

    def _match(self, lhs: NumberType):
        return self._equal(lhs)
