from mega.match.values.base import HigherOrderValue
from mega.match.values.type import is_mapping, MappingType


class Mapping(HigherOrderValue):

    @classmethod
    def accepts_rhs(cls, rhs):
        return is_mapping(rhs)

    def _needs_casting(self, value, function_type=None):
        return False

    def _cast(self, value, function_type=None, reference_value=None):
        pass

    def _equal(self, lhs: MappingType):
        return self._match(lhs)

    def _match(self, lhs: MappingType):
        if lhs is None:
            return not self.rhs

        for key in self.rhs:
            if key in lhs:
                if not self._evaluate(lhs[key], self.rhs[key]):
                    return False

        return True
