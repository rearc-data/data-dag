from typing import Any, Dict, Optional

import pytest
from airflow.operators.dummy import DummyOperator

from data_dag.operator_factory import (
    OperatorFactory,
    SimpleOperatorFactory,
    SimpleOperatorComponent,
)


def test_basic():
    class SampleOp(OperatorFactory):
        to_add: float

        def make_operator(self, *args, **kwargs):
            return DummyOperator(task_id=f"Add_{self.to_add}")

    data = {"to_add": 5}
    op = SampleOp.parse_obj(data)
    airflow_op = op.make_operator()
    assert isinstance(airflow_op, DummyOperator)
    assert airflow_op.task_id == "Add_5.0"


def test_failure_direct_instantiation():
    pytest.raises(NotImplementedError, OperatorFactory.parse_obj({}).make_operator)


def test_simple():
    """Tests that SimpleOperatorFactory works as intended"""

    class SampleOp(SimpleOperatorFactory):
        i: int
        flag: bool = True

        def make_operator(self):
            raise NotImplementedError()

    op = SampleOp.parse_obj(5)
    assert isinstance(op, SampleOp)
    assert op.i == 5
    assert op.flag is True

    op = SampleOp.parse_obj({"i": 5})
    assert isinstance(op, SampleOp)
    assert op.i == 5
    assert op.flag is True

    op = SampleOp.parse_obj({"i": 5, "flag": False})
    assert isinstance(op, SampleOp)
    assert op.i == 5
    assert op.flag is False


def test_simple_failure_multiple_fields():
    with pytest.raises(TypeError):

        class _SampleOp(SimpleOperatorComponent):
            a: int
            b: int


def test_simple_failure_dict_field():
    class SampleOp(SimpleOperatorComponent):
        d: Dict[str, Any]

    with pytest.raises(NotImplementedError):
        SampleOp.parse_obj(dict())


def test_none_not_the_same_as_undefined():
    class SampleOp(SimpleOperatorComponent):
        a: int
        b: Optional[int]  # Defaults to None, so there's only one undefined field
