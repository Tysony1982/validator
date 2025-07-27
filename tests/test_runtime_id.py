from src.expectations.validators.base import ValidatorBase

class _Dummy(ValidatorBase):
    @classmethod
    def kind(cls):
        return "custom"

    def custom_sql(self, table: str):
        return "SELECT 1"

    def interpret(self, value):
        return True


def test_runtime_id_unique_large_sample():
    ids = {_Dummy().runtime_id for _ in range(1_000_000)}
    assert len(ids) == 1_000_000
    assert all(len(i) < 63 for i in ids)
