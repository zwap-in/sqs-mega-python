import re
from datetime import date, datetime
from typing import Optional, Union

import dateutil.parser
from dateutil.tz import tzutc

from mega.match.types import is_datetime, is_string, DateTimeType, ValueType
from mega.match.values.value import ComparableValue


def _match_iso_date(string: str) -> re.Match:
    return re.match(r'^(\d{4})-(\d{1,2})-(\d{1,2})$', string)


def _parse_iso_date(string: str) -> Optional[date]:
    match = _match_iso_date(string)
    if match:
        return date(
            year=int(match.group(1)),
            month=int(match.group(2)),
            day=int(match.group(3))
        )
    return None


class DateTime(ComparableValue):

    def __init__(self, rhs: Union[DateTimeType, str]):
        super().__init__(self.__normalize_rhs(rhs))

    @classmethod
    def accepts_rhs(cls, value):
        return is_datetime(value) or is_string(value)

    def _needs_casting(self, value, function_type=None):
        return is_string(value)

    def _cast(self, value, function_type=None, reference_value=None):
        default_attributes = self.__default_datetime_attributes(value, reference_value)

        try:
            dt = dateutil.parser.parse(value, default=default_attributes)
            return self.__normalize_native_datetime(dt)
        except dateutil.parser.ParserError:
            raise ValueError('Not a valid ISO-8601 string: "{}"'.format(str(value)))

    def _equal(self, lhs: DateTimeType):
        return self._match(lhs)

    def _match(self, lhs: DateTimeType):
        if lhs is None:
            return False

        lhs = self.__normalize_value(lhs)
        lhs, rhs = self.__cast_to_same_native_type(lhs, self.rhs)

        return (lhs - rhs).total_seconds() == 0

    def _less_than(self, lhs: DateTimeType):
        lhs = self.__normalize_value(lhs)
        lhs, rhs = self.__cast_to_same_native_type(lhs, self.rhs)

        return lhs < rhs

    def __normalize_rhs(self, rhs: ValueType) -> ValueType:
        if is_string(rhs):
            return _parse_iso_date(rhs) or rhs
        return self.__normalize_value(rhs)

    def __normalize_value(self, value: ValueType) -> ValueType:
        if type(value) is datetime:
            return self.__normalize_native_datetime(value)
        return value

    @staticmethod
    def __normalize_native_datetime(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=tzutc(), microsecond=0)
        return dt.replace(microsecond=0)

    def __default_datetime_attributes(
            self, value_to_cast: ValueType, reference_value: DateTimeType) -> Optional[datetime]:
        if type(reference_value) is not datetime:
            return None

        if self.__is_iso_date_string(value_to_cast):
            return reference_value

        return reference_value.replace(tzinfo=tzutc())

    @staticmethod
    def __is_iso_date_string(string: str) -> bool:
        return bool(_match_iso_date(string))

    @classmethod
    def __cast_to_same_native_type(cls, lhs: DateTimeType, rhs: DateTimeType) -> (DateTimeType, DateTimeType):
        if type(lhs) is date and type(rhs) is datetime:
            lhs = cls.__date_to_datetime(lhs, reference=rhs)
        elif type(lhs) is datetime and type(rhs) is date:
            rhs = cls.__date_to_datetime(rhs, reference=lhs)
        return lhs, rhs

    @staticmethod
    def __date_to_datetime(date_: date, reference: datetime) -> datetime:
        return datetime(
            year=date_.year,
            month=date_.month,
            day=date_.day,
            hour=reference.hour,
            minute=reference.minute,
            second=reference.second,
            microsecond=reference.microsecond,
            tzinfo=reference.tzinfo
        )
