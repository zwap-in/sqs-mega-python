from abc import ABC, abstractmethod
from typing import Set, Type, Any, Optional

from mega.match.values.type import Value, is_scalar


class RightHandSideValue(ABC):
    class FunctionType:
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

    def _accepts_lhs(self, lhs: Value, function_type: str) -> bool:
        if lhs is None:
            return True
        return self.accepts_rhs(lhs)

    @abstractmethod
    def _needs_casting(self, value: Value, function_type: Optional[str] = None) -> bool:
        pass

    @abstractmethod
    def _cast(
            self, value: Value,
            reference_value: Optional[Value] = None,
            function_type: Optional[str] = None
    ) -> Value:
        pass

    def _filter_rhs(self, rhs: Value) -> Value:
        if not self.accepts_rhs(rhs):
            raise RightHandSideTypeError(type(self), rhs)

        if self._needs_casting(rhs):
            try:
                return self._cast(rhs, function_type=None, reference_value=None)
            except Exception as e:
                raise RightHandSideTypeError(type(self), rhs, context=e)

        return rhs

    def _filter_lhs(self, lhs: Value, function_type: str) -> Value:
        if not self._accepts_lhs(lhs, function_type):
            raise LeftHandSideTypeError(self, function_type, lhs)

        if self._needs_casting(lhs):
            try:
                return self._cast(lhs, function_type=function_type, reference_value=self.rhs)
            except Exception as e:
                raise LeftHandSideTypeError(self, function_type, lhs, context=e)

        return lhs

    def equal(self, lhs: Value) -> bool:
        lhs = self._filter_lhs(lhs, self.FunctionType.EQUAL)
        return self._equal(lhs)

    @abstractmethod
    def _equal(self, lhs: Value) -> bool:
        pass

    def match(self, lhs: Value) -> bool:
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


class HigherOrderValue(RightHandSideValue, ABC):
    def _evaluate(self, lhs, rhs):
        # Because of a cyclic dependency that proved impossible to solve, we must use meta-programming here
        eval('from mega.match.evaluation import evaluate; evaluate(lhs, rhs)')


class RightHandSideTypeError(Exception):
    def __init__(self, value_type: Type[RightHandSideValue], rhs: Value, context=None):
        message = '[{0}] Invalid right-hand side <{1}> ({2}).'.format(
            value_type.__name__,
            type(rhs).__name__,
            rhs if is_scalar(rhs) else '[…]'
        )
        if context:
            message = '{0} {1}'.format(message, context)
        super().__init__(message)

        self.value_type = value_type
        self.rhs = rhs


class LeftHandSideTypeError(Exception):
    def __init__(self, value: RightHandSideValue, function_type: str, lhs: Value, context=None):
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

        self.value = value
        self.function_type = function_type
        self.lhs = lhs
