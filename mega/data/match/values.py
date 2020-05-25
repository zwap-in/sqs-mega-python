import decimal
import re
from abc import ABC, abstractmethod
from datetime import datetime, date, tzinfo
from enum import Enum
from typing import Optional, Type, Set, Any

import dateutil.parser

from mega.data.match.functions import evaluate
from mega.data.match.types import Value, DateTimeType, is_number, is_string, is_datetime, \
    is_boolean, is_scalar, NumberType, is_collection, is_mapping, MappingType


class RightHandSideValue(ABC):
    class FunctionType(Enum):
        EQUAL = 'equal'
        MATCH = 'match'

    @classmethod
    @abstractmethod
    def accepts_rhs(cls, rhs: Value) -> bool:
        pass

    @classmethod
    def _matches_type(cls, value: Value, types: Set[Type[Any]]) -> bool:
        if not types:
            return False

        return any(
            isinstance(value, t)
            for t in types
        )

    def __init__(self, rhs: Value):
        self._rhs = self._filter_rhs(rhs)

    @property
    def rhs(self) -> Value:
        return self._rhs

    def _accepts_lhs(self, lhs: Value, function_type: FunctionType) -> bool:
        if lhs is None:
            return True
        return self.accepts_rhs(lhs)

    @abstractmethod
    def _needs_casting(self, value: Value, function_type: Optional[FunctionType] = None) -> bool:
        pass

    @abstractmethod
    def _cast(self, value: Value, reference_type: Optional[Type] = None) -> Value:
        return value

    def _filter_rhs(self, rhs: Value) -> Value:
        if not self.accepts_rhs(rhs):
            raise RightHandSideTypeError(type(self), rhs)

        if self._needs_casting(rhs):
            try:
                return self._cast(rhs, reference_type=None)
            except Exception as e:
                raise RightHandSideTypeError(type(self), rhs, context=e)

        return rhs

    def _filter_lhs(self, lhs: Value, function_type: FunctionType) -> Value:
        if not self._accepts_lhs(lhs, function_type):
            raise LeftHandSideTypeError(self, function_type.value, lhs)

        if self._needs_casting(lhs):
            try:
                return self._cast(lhs, reference_type=type(self.rhs))
            except Exception as e:
                raise LeftHandSideTypeError(self, function_type.value, lhs, context=e)

        return lhs

    def equal(self, lhs: Value) -> bool:
        lhs = self._filter_lhs(lhs, self.FunctionType.EQUAL)
        return self._equal(lhs)

    @abstractmethod
    def _equal(self, lhs: Value) -> bool:
        pass

    def match(self, lhs: Value) -> bool:
        if lhs is None:
            return self.rhs is None

        lhs = self._filter_lhs(lhs, self.FunctionType.MATCH)
        return self._match(lhs)

    @abstractmethod
    def _match(self, lhs: Value) -> bool:
        pass


class ComparableValue(RightHandSideValue, ABC):
    class FunctionType(RightHandSideValue.FunctionType):
        LESS_THAN = 'less_than'
        LESS_THAN_OR_EQUAL = 'less_than_or_equal'
        GREATER_THAN = 'greater_than'
        GREATER_THAN_OR_EQUAL = 'greater_than_or_equal'

    def _accepts_lhs(self, lhs, function_type):
        if lhs is None:
            return function_type in (self.FunctionType.EQUAL, self.FunctionType.MATCH)
        return self.accepts_rhs(lhs)

    def less_than(self, lhs: Value) -> bool:
        lhs = self._filter_lhs(lhs, self.FunctionType.LESS_THAN)
        return self._less_than(lhs)

    @abstractmethod
    def _less_than(self, lhs: Value) -> bool:
        pass

    def less_than_or_equal(self, lhs: Value) -> bool:
        lhs = self._filter_lhs(lhs, self.FunctionType.LESS_THAN_OR_EQUAL)
        return self._less_than(lhs) or self._equal(lhs)

    def greater_than(self, lhs: Value) -> bool:
        lhs = self._filter_lhs(lhs, self.FunctionType.GREATER_THAN)
        return not (self._less_than(lhs) or self._equal(lhs))

    def greater_than_or_equal(self, lhs: Value) -> bool:
        lhs = self._filter_lhs(lhs, self.FunctionType.GREATER_THAN_OR_EQUAL)
        return not self._less_than(lhs)

    def _filter_lhs(self, lhs, function_type):
        if lhs is None:
            raise LeftHandSideTypeError(self, function_type.value, lhs, context='Cannot compare with None.')
        return super()._filter_lhs(lhs, function_type)


class Null(RightHandSideValue):
    @classmethod
    def accepts_rhs(cls, rhs) -> bool:
        return rhs is None

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

    def _needs_casting(self, value, function_type=None):
        return False

    def _cast(self, value, reference_type=None):
        pass

    def _equal(self, lhs):
        if lhs is None:
            return False

        return lhs == self.rhs

    def _match(self, lhs):
        if lhs is None:
            return not self.rhs

        return self._equal(lhs)


class Collection(RightHandSideValue):
    class FunctionType(RightHandSideValue.FunctionType):
        CONTAINS = 'contains'

    @classmethod
    def accepts_rhs(cls, rhs):
        return is_collection(rhs)

    def _accepts_lhs(self, lhs, function_type):
        if function_type is self.FunctionType.CONTAINS:
            return is_scalar(lhs)
        elif function_type in (self.FunctionType.EQUAL, self.FunctionType.MATCH):
            return is_collection(lhs)
        return False

    def _needs_casting(self, value, function_type=None):
        return False

    def _cast(self, value, reference_type=None):
        pass

    def _equal(self, lhs: Value) -> bool:
        if lhs is None:
            return not self.rhs

        return self._compare_collections(lhs, self.rhs, match_all_lhs_items=True)

    def _match(self, lhs: Value) -> bool:
        if is_scalar(lhs):
            return self._contains(lhs)

        return self._compare_collections(lhs, self.rhs, match_all_lhs_items=False)

    def contains(self, lhs: Value) -> bool:
        lhs = self._filter_lhs(lhs, self.FunctionType.CONTAINS)
        return self._contains(lhs)

    def _contains(self, lhs: Value) -> bool:
        for rhs_item in self.rhs:
            try:
                if evaluate(rhs_item, lhs):
                    return True
            except LeftHandSideTypeError:
                pass

        return False

    @staticmethod
    def _compare_collections(lhs, rhs, match_all_lhs_items):
        lhs_matches = {
            lhs_item: False
            for lhs_item in lhs
        }

        for rhs_item in rhs:
            match = False

            for lhs_item in lhs:
                if lhs_matches[lhs_item]:
                    match = True
                    break

                if evaluate(lhs_item, rhs_item):
                    lhs_matches[lhs_item] = True
                    match = True
                    break

            if not match:
                return False

        if match_all_lhs_items:
            for match in lhs_matches.values():
                if match is False:
                    return False

        return True


class Mapping(RightHandSideValue):

    @classmethod
    def accepts_rhs(cls, rhs):
        return is_mapping(rhs)

    def _needs_casting(self, value, function_type=None):
        return False

    def _cast(self, value, reference_type=None):
        pass

    def _equal(self, lhs: MappingType):
        return self._match(lhs)

    def _match(self, lhs: MappingType):
        if lhs is None:
            return not self.rhs

        for key in self.rhs:
            if key in lhs:
                if not evaluate(lhs[key], self.rhs[key]):
                    return False

        return True


class RightHandSideTypeError(Exception):
    def __init__(self, rhs_value_type: Type['RightHandSideValue'], rhs: Value, context=None):
        message = '[{0}] Invalid right-hand side with type <{1}> ({2}).'.format(
            rhs_value_type.__name__,
            type(rhs).__name__,
            rhs if is_scalar(rhs) else '[…]'
        )
        if context:
            message = '{0} {1}'.format(message, context)
        super().__init__(message)


class LeftHandSideTypeError(Exception):
    def __init__(self, value: 'RightHandSideValue', function_type: str, lhs: Value, context=None):
        rhs = value.rhs
        message = (
            '[{0}.{1}] Could not apply left-hand side <{2}> ({3}) to right-hand side <{4}> ({5}).'.format(
                type(value).__name__,
                function_type,
                type(lhs).__name__,
                lhs if is_scalar(lhs) else '[…]',
                type(rhs).__name__,
                rhs if is_scalar(rhs) else '[…]'
            )
        )
        if context:
            message = '{0} {1}'.format(message, context)
        super().__init__(message)
