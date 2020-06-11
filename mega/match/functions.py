from abc import ABC, abstractmethod
from typing import Union

from mega.match.values.build import value, comparable_value, collection_value
from mega.match.values.collection import Collection
from mega.match.values.types import Value, CollectionType
from mega.match.values.value import RightHandSideValue, ComparableValue


class RightHandSideFunction(ABC):
    @abstractmethod
    def evaluate(self, lhs: Value) -> bool:
        pass


def identity(rhs: Union[Value, RightHandSideValue, RightHandSideFunction]) -> RightHandSideFunction:
    if isinstance(rhs, RightHandSideFunction):
        return rhs
    return Equal(rhs)


class ValueFunction(RightHandSideFunction, ABC):
    def __init__(self, rhs: Union[Value, RightHandSideValue]):
        self.rhs = value(rhs)


class Equal(ValueFunction):
    def evaluate(self, lhs: Value) -> bool:
        return self.rhs.equal(lhs)


class Match(ValueFunction):
    def evaluate(self, lhs: Value) -> bool:
        return self.rhs.match(lhs)


class ComparisonFunction(ValueFunction, ABC):
    def __init__(self, rhs: Union[Value, ComparableValue]):
        super().__init__(rhs)
        self.rhs = comparable_value(self.rhs)


class GreaterThan(ComparisonFunction):
    def evaluate(self, lhs: Value) -> bool:
        return self.rhs.greater_than(lhs)


class GreaterThanOrEqual(ComparisonFunction):
    def evaluate(self, lhs: Value) -> bool:
        return self.rhs.greater_than_or_equal(lhs)


class LessThan(ComparisonFunction):
    def evaluate(self, lhs: Value) -> bool:
        return self.rhs.less_than(lhs)


class LessThanOrEqual(ComparisonFunction):
    def evaluate(self, lhs: Value) -> bool:
        return self.rhs.less_than_or_equal(lhs)


class HigherOrderFunction(RightHandSideFunction, ABC):
    def __init__(self, rhs: RightHandSideFunction):
        if not isinstance(rhs, RightHandSideFunction):
            raise TypeError
        self.rhs = rhs


class Not(HigherOrderFunction):

    def __init__(self, rhs: Union[Value, RightHandSideValue, RightHandSideFunction]):
        super().__init__(identity(rhs))

    def evaluate(self, lhs: Value) -> bool:
        return not self.rhs.evaluate(lhs)


class CombinedFunction(RightHandSideFunction, ABC):
    def __init__(self, *rhs: CollectionType[RightHandSideFunction]):
        if len(rhs) < 2:
            raise ValueError

        for i in rhs:
            if not isinstance(i, RightHandSideFunction):
                raise TypeError

        self.rhs = rhs


class And(CombinedFunction):
    def evaluate(self, lhs: Value) -> bool:
        return all(
            i.evaluate(lhs)
            for i in self.rhs
        )


class Or(CombinedFunction):
    def evaluate(self, lhs: Value) -> bool:
        return any(
            i.evaluate(lhs)
            for i in self.rhs
        )


class CollectionFunction(ValueFunction, ABC):
    def __init__(self, rhs: Union[CollectionType, Collection]):
        super().__init__(rhs)
        self.rhs = collection_value(self.rhs)


class In(CollectionFunction):
    def evaluate(self, lhs: Value) -> bool:
        return self.rhs.contains(lhs)


def eq(rhs) -> Equal:
    return Equal(rhs)


def neq(rhs) -> Not:
    return Not(rhs)


def gt(rhs) -> GreaterThan:
    return GreaterThan(rhs)


def gte(rhs) -> GreaterThanOrEqual:
    return GreaterThanOrEqual(rhs)


def lt(rhs) -> LessThan:
    return LessThan(rhs)


def lte(rhs) -> LessThanOrEqual:
    return LessThanOrEqual(rhs)


def not_(rhs) -> Not:
    return Not(rhs)


def and_(*rhs) -> And:
    return And(rhs)


def or_(*rhs) -> Or:
    return Or(rhs)


def in_(*rhs) -> In:
    return In(rhs)


def match(rhs) -> Match:
    return Match(rhs)
