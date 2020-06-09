from mega.data.match.evaluation import evaluate
from mega.data.match.values.types import is_mapping, MappingType
from mega.data.match.values.value import RightHandSideValue


class Mapping(RightHandSideValue):

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
                if not evaluate(lhs[key], self.rhs[key]):
                    return False

        return True
