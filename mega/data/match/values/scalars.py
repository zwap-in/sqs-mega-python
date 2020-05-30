import decimal
import re
from datetime import datetime, date, tzinfo

import dateutil.parser

from mega.data.match.values.types import DateTimeType, is_number, is_string, is_datetime, \
    is_boolean, NumberType
from mega.data.match.values.value import RightHandSideValue, ComparableValue


class Null(RightHandSideValue):
    @classmethod
    def accepts_rhs(cls, rhs) -> bool:
        return rhs is None

    @classmethod
    def _accepts_lhs(cls, lhs, function_type) -> bool:
        return True

    def _needs_casting(self, value, function_type=None):
        return False

    def _cast(self, value, reference_type=None):
        pass

    def _equal(self, lhs):
        return lhs is None

    def _match(self, lhs):
        return not lhs


class String(RightHandSideValue):

    @classmethod
    def accepts_rhs(cls, rhs):
        return is_string(rhs)

    def _accepts_lhs(self, lhs, function_type):
        return lhs is None or \
               is_string(lhs) or \
               is_number(lhs)

    def _needs_casting(self, value, function_type=None):
        return is_number(value)

    def _cast(self, value, reference_type=None):
        return str(value)

    def _equal(self, lhs: str):
        if lhs is None:
            return not self.rhs

        return lhs == self.rhs

    def _match(self, lhs: str):
        if lhs is None:
            return not self.rhs

        return bool(re.match(self.rhs, lhs))


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


class DateTime(ComparableValue):

    @classmethod
    def accepts_rhs(cls, value):
        return is_datetime(value) or is_string(value)

    def _needs_casting(self, value, function_type=None):
        return is_string(value)

    def _cast(self, value, reference_type=None):
        try:
            return dateutil.parser.parse(value)
        except dateutil.parser.ParserError:
            raise ValueError('Not a valid ISO-8601 string: ' + str(value))

    def _equal(self, lhs: DateTimeType):
        if lhs is None:
            return False

        lhs, rhs = self.__normalized_values(lhs)
        return lhs == rhs

    def _match(self, lhs: DateTimeType):
        return self._equal(lhs)

    def _less_than(self, lhs: DateTimeType):
        lhs, rhs = self.__normalized_values(lhs)
        return lhs < rhs

    def __normalized_values(self, lhs):
        rhs = self.rhs
        if type(lhs) is date and type(rhs) is datetime:
            lhs = self.__date_to_datetime(lhs, rhs.tzinfo)
        elif type(lhs) is datetime and type(rhs) is date:
            rhs = self.__date_to_datetime(rhs, lhs.tzinfo)
        return lhs, rhs

    @staticmethod
    def __date_to_datetime(date_: date, timezone: tzinfo) -> datetime:
        return datetime(
            year=date_.year,
            month=date_.month,
            day=date_.day,
            tzinfo=timezone
        )


class Boolean(RightHandSideValue):

    @classmethod
    def accepts_rhs(cls, rhs):
        return is_boolean(rhs)

    def _accepts_lhs(self, lhs, function_type) -> bool:
        if function_type is self.FunctionType.EQUAL:
            return lhs is None or self.accepts_rhs(lhs)
        if function_type is self.FunctionType.MATCH:
            return True
        raise AssertionError(function_type)

    def _needs_casting(self, value, function_type=None):
        return False

    def _cast(self, value, reference_type=None):
        pass

    def _equal(self, lhs):
        if lhs is None:
            return False

        return lhs == self.rhs

    def _match(self, lhs):
        if is_string(lhs) and lhs.lower() == 'false':
            return not self.rhs

        return bool(lhs) is self.rhs
