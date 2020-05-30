from datetime import date, datetime, tzinfo

import dateutil.parser

from mega.data.match.values.types import is_datetime, is_string, DateTimeType
from mega.data.match.values.value import ComparableValue


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