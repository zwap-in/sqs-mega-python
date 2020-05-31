from mega.data.match.evaluation import evaluate
from mega.data.match.values.types import is_collection, is_scalar, Value
from mega.data.match.values.value import RightHandSideValue, LeftHandSideTypeError


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

    def _cast(self, value, function_type=None, reference_value=None):
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