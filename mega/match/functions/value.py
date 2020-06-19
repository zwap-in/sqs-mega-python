from abc import ABC

from mega.match.types import RightHandSideValueType, ComparableRightHandSideValueType, CollectionRightHandSideValueType, \
    RightHandSideFunction
from mega.match.values.build import value, comparable_value, collection_value


class ValueFunction(RightHandSideFunction, ABC):
    def __init__(self, rhs: RightHandSideValueType):
        self.rhs = value(rhs)


class ComparisonFunction(ValueFunction, ABC):
    def __init__(self, rhs: ComparableRightHandSideValueType):
        super().__init__(rhs)
        self.rhs = comparable_value(self.rhs)


class CollectionFunction(ValueFunction, ABC):
    def __init__(self, rhs: CollectionRightHandSideValueType):
        super().__init__(rhs)
        self.rhs = collection_value(self.rhs)
