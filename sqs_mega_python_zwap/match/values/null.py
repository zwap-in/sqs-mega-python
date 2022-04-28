from sqs_mega_python_zwap.match.values.value import Value


class Null(Value):
    def __init__(self):
        super().__init__(None)

    @classmethod
    def accepts_rhs(cls, rhs) -> bool:
        return rhs is None

    @classmethod
    def _accepts_lhs(cls, lhs, function_type) -> bool:
        return True

    def _needs_casting(self, value, function_type=None):
        return False

    def _cast(self, value, function_type=None, reference_value=None):
        pass

    def _equal(self, lhs):
        return lhs is None

    def _match(self, lhs):
        return not lhs
