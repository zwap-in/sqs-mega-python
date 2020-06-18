from collections import defaultdict

from mega.match.values.base import RightHandSideValue, LeftHandSideTypeError, HigherOrderValue
from mega.match.values.types import is_collection, is_scalar, ValueType


class Collection(HigherOrderValue):
    class FunctionType(RightHandSideValue.FunctionType):
        CONTAINS = 'contains'

    @classmethod
    def accepts_rhs(cls, rhs):
        return is_collection(rhs)

    def _accepts_lhs(self, lhs, function_type):
        if function_type is self.FunctionType.CONTAINS:
            return is_scalar(lhs)
        elif function_type is self.FunctionType.EQUAL:
            return lhs is None or is_collection(lhs)
        elif function_type is self.FunctionType.MATCH:
            return is_scalar(lhs) or is_collection(lhs)
        return False

    def _needs_casting(self, value, function_type=None):
        return False

    def _cast(self, value, function_type=None, reference_value=None):
        pass

    def _equal(self, lhs: ValueType) -> bool:
        if lhs is None:
            return not self.rhs

        return self._compare_collections(lhs, self.rhs, self.FunctionType.EQUAL)

    def _match(self, lhs: ValueType) -> bool:
        if lhs is None:
            return not self.rhs

        if is_scalar(lhs):
            return self._contains(lhs, function_type=self.FunctionType.MATCH)

        return self._compare_collections(lhs, self.rhs, self.FunctionType.MATCH)

    def contains(self, lhs: ValueType) -> bool:
        lhs = self._filter_lhs(lhs, self.FunctionType.CONTAINS)
        return self._contains(lhs)

    def _contains(self, lhs: ValueType, function_type=FunctionType.CONTAINS) -> bool:
        for rhs_item in self.rhs:
            try:
                if self._evaluate(lhs, rhs_item):
                    return True
            except LeftHandSideTypeError as e:
                raise LeftHandSideTypeError(
                    self, function_type, lhs,
                    context='Left-hand side is not compatible with collection type. {}'.format(e)
                ) from e

        return False

    def _compare_collections(self, lhs, rhs, function_type):
        lhs_matches = defaultdict(lambda: set())

        for rhs_item in rhs:
            rhs_match = False

            for lhs_item in lhs:
                if rhs_item in lhs_matches[lhs_item]:
                    rhs_match = True
                    break

                try:
                    if self._evaluate(lhs_item, rhs_item):
                        lhs_matches[lhs_item].add(rhs_item)
                        rhs_match = True
                        break
                except LeftHandSideTypeError as e:
                    raise LeftHandSideTypeError(
                        self, function_type, lhs,
                        context='Collections have incompatible types. {}'.format(e)
                    ) from e

            if not rhs_match:
                return False

        if function_type == self.FunctionType.EQUAL:
            for lhs_item in lhs:
                if not lhs_matches[lhs_item]:
                    return False

        return True
