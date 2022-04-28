from sqs_mega_python_zwap.match.types import is_boolean, is_string, BooleanType
from sqs_mega_python_zwap.match.values.value import Value


class Boolean(Value):

    def __init__(self, rhs: BooleanType):
        super().__init__(rhs)

    @classmethod
    def accepts_rhs(cls, rhs):
        return is_boolean(rhs)

    def _accepts_lhs(self, lhs, function_type) -> bool:
        if function_type is self.FunctionType.EQUAL:
            return lhs is None or self.accepts_rhs(lhs)
        if function_type is self.FunctionType.MATCH:
            return True
        raise AssertionError(function_type)

    def _needs_casting(self, value, function_type=None):
        return False

    def _cast(self, value, function_type=None, reference_value=None):
        pass

    def _equal(self, lhs):
        if lhs is None:
            return False

        return lhs == self.rhs

    def _match(self, lhs):
        if is_string(lhs) and lhs.lower() == 'false':
            return not self.rhs

        return bool(lhs) is self.rhs
