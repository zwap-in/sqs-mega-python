from abc import ABC, abstractmethod
from typing import Set, Type, Any, Optional

from mega.match.types import ValueType, is_scalar, RightHandSideValue, ComparableRightHandSideValue, \
    RightHandSideType, ComparableType


class Value(RightHandSideValue, ABC):
    class FunctionType:
        EQUAL = 'equal'
        MATCH = 'match'

    def __init__(self, rhs: ValueType):
        self._rhs = self._filter_rhs(rhs)

    @property
    def rhs(self) -> ValueType:
        return self._rhs

    @classmethod
    @abstractmethod
    def accepts_rhs(cls, rhs: ValueType) -> bool:
        pass

    @classmethod
    def _matches_type(cls, value: ValueType, types: Set[Type[Any]]) -> bool:
        if not types:
            return False

        return any(
            isinstance(value, t)
            for t in types
        )

    def _accepts_lhs(self, lhs: ValueType, function_type: str) -> bool:
        if lhs is None:
            return True
        return self.accepts_rhs(lhs)

    @abstractmethod
    def _needs_casting(self, value: ValueType, function_type: Optional[str] = None) -> bool:
        pass

    @abstractmethod
    def _cast(
            self, value: ValueType,
            reference_value: Optional[ValueType] = None,
            function_type: Optional[str] = None
    ) -> ValueType:
        pass

    def _filter_rhs(self, rhs: ValueType) -> ValueType:
        if not self.accepts_rhs(rhs):
            raise RightHandSideTypeError(type(self), rhs)

        if self._needs_casting(rhs):
            try:
                return self._cast(rhs, function_type=None, reference_value=None)
            except Exception as e:
                raise RightHandSideTypeError(type(self), rhs, context=e)

        return rhs

    def _filter_lhs(self, lhs: ValueType, function_type: str) -> ValueType:
        if not self._accepts_lhs(lhs, function_type):
            raise LeftHandSideTypeError(self, function_type, lhs)

        if self._needs_casting(lhs):
            try:
                return self._cast(lhs, function_type=function_type, reference_value=self.rhs)
            except Exception as e:
                raise LeftHandSideTypeError(self, function_type, lhs, context=e)

        return lhs

    def equal(self, lhs: ValueType) -> bool:
        lhs = self._filter_lhs(lhs, self.FunctionType.EQUAL)
        return self._equal(lhs)

    @abstractmethod
    def _equal(self, lhs: ValueType) -> bool:
        pass

    def match(self, lhs: ValueType) -> bool:
        lhs = self._filter_lhs(lhs, self.FunctionType.MATCH)
        return self._match(lhs)

    @abstractmethod
    def _match(self, lhs: ValueType) -> bool:
        pass


class ComparableValue(Value, ComparableRightHandSideValue, ABC):
    class FunctionType(Value.FunctionType):
        LESS_THAN = 'less_than'
        LESS_THAN_OR_EQUAL = 'less_than_or_equal'
        GREATER_THAN = 'greater_than'
        GREATER_THAN_OR_EQUAL = 'greater_than_or_equal'

    def __init__(self, rhs: ComparableType):
        super().__init__(rhs)

    def _accepts_lhs(self, lhs, function_type):
        if lhs is None:
            return function_type in (self.FunctionType.EQUAL, self.FunctionType.MATCH)
        return self.accepts_rhs(lhs)

    def less_than(self, lhs: ValueType) -> bool:
        lhs = self._filter_lhs(lhs, self.FunctionType.LESS_THAN)
        return self._less_than(lhs)

    @abstractmethod
    def _less_than(self, lhs: ValueType) -> bool:
        pass

    def less_than_or_equal(self, lhs: ValueType) -> bool:
        lhs = self._filter_lhs(lhs, self.FunctionType.LESS_THAN_OR_EQUAL)
        return self._less_than(lhs) or self._equal(lhs)

    def greater_than(self, lhs: ValueType) -> bool:
        lhs = self._filter_lhs(lhs, self.FunctionType.GREATER_THAN)
        return not (self._less_than(lhs) or self._equal(lhs))

    def greater_than_or_equal(self, lhs: ValueType) -> bool:
        lhs = self._filter_lhs(lhs, self.FunctionType.GREATER_THAN_OR_EQUAL)
        return not self._less_than(lhs)


class HigherOrderValue(Value, ABC):
    @staticmethod
    def _evaluate(lhs: ValueType, rhs: RightHandSideType) -> bool:
        #
        # PLEASE NOTE: because of a cyclic dependency between mega.match.values and mega.match.function modules, we
        # must use meta-programming here. The alternative would be squashing everything in the same file!
        #
        exec('from mega.match.evaluation import evaluate; result = evaluate(lhs, rhs)')
        return locals()['result']


class RightHandSideTypeError(Exception):
    def __init__(self, rhs_type: Type[Value], rhs_value, context=None):
        message = '[{0}] Invalid right-hand side <{1}> ({2}).'.format(
            rhs_type.__name__,
            type(rhs_value).__name__,
            rhs_value if is_scalar(rhs_value) else '[…]'
        )
        if context:
            message = '{0} {1}'.format(message, context)
        super().__init__(message)
        self.rhs_type = rhs_type
        self.rhs_value = rhs_value


class LeftHandSideTypeError(Exception):
    def __init__(self, rhs: Value, function_type: str, lhs: ValueType, context=None):
        rhs_value = rhs.rhs
        message = (
            '[{0}.{1}] Could not apply left-hand side <{2}> ({3}) to right-hand side <{4}> ({5}).'.format(
                type(rhs).__name__,
                function_type,
                type(lhs).__name__,
                lhs if is_scalar(lhs) else '[…]',
                type(rhs_value).__name__,
                rhs_value if is_scalar(rhs_value) else '[…]'
            )
        )
        if context:
            message = '{0} {1}'.format(message, context)

        super().__init__(message)

        self.rhs = rhs
        self.function_type = function_type
        self.lhs = lhs
