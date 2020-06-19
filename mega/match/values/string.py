import re

from mega.match.types import StringType, is_string
from mega.match.values.value import Value


class String(Value):

    def __init__(self, rhs: StringType):
        super().__init__(rhs)

    @classmethod
    def accepts_rhs(cls, rhs):
        return is_string(rhs)

    def _accepts_lhs(self, lhs, function_type):
        return lhs is None or is_string(lhs)

    def _needs_casting(self, value, function_type=None):
        return False

    def _cast(self, value, function_type=None, reference_value=None):
        pass

    def _equal(self, lhs: str):
        if lhs is None:
            return not self.rhs

        return lhs == self.rhs

    def _match(self, lhs: str):
        if lhs is None:
            return not self.rhs

        return bool(re.match(self.rhs, lhs))
