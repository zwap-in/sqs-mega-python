from abc import ABC, abstractmethod
from typing import Union

from mega.match.values.base import RightHandSideValue, ComparableValue
from mega.match.values.build import value, comparable_value, collection_value
from mega.match.values.collection import Collection
from mega.match.values.type import Value, CollectionType


class RightHandSideFunction(ABC):
    @abstractmethod
    def evaluate(self, lhs: Value) -> bool:
        pass


class ValueFunction(RightHandSideFunction, ABC):
    def __init__(self, rhs: Union[Value, RightHandSideValue]):
        self.rhs = value(rhs)


class ComparisonFunction(ValueFunction, ABC):
    def __init__(self, rhs: Union[Value, ComparableValue]):
        super().__init__(rhs)
        self.rhs = comparable_value(self.rhs)


class CollectionFunction(ValueFunction, ABC):
    def __init__(self, rhs: Union[CollectionType, Collection]):
        super().__init__(rhs)
        self.rhs = collection_value(self.rhs)


class CombinedFunction(RightHandSideFunction, ABC):
    def __init__(self, *rhs: CollectionType[RightHandSideFunction]):
        if len(rhs) < 2:
            raise ValueError

        for i in rhs:
            if not isinstance(i, RightHandSideFunction):
                raise TypeError

        self.rhs = rhs


class HigherOrderFunction(RightHandSideFunction, ABC):
    def __init__(self, rhs: RightHandSideFunction):
        if not isinstance(rhs, RightHandSideFunction):
            raise TypeError
        self.rhs = rhs
