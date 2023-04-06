from datetime import datetime
from typing import Any, Dict, Optional

import pytest
from airflow.operators.dummy import DummyOperator

from data_dag.operator_factory import (
    OperatorFactory,
    SimpleOperatorComponent,
    SimpleOperatorFactory,
)
from data_dag.operator_factory.base import BaseOperatorFactory

_junk_dag_kwargs = dict(
    dag_id="junk",
    schedule_interval="@daily",
    start_date=datetime.today(),
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


def test_not_implemented1():
    class SampleOp(OperatorFactory):
        pass

    with pytest.raises(NotImplementedError):
        SampleOp().default_task_id


def test_not_implemented2():
    class SampleOp(OperatorFactory):
        @property
        def default_task_id(self) -> str:
            return "junk"

    from data_dag.dag_factory import DagFactory

    class SampleDag(DagFactory):
        def _make_dag(self):
            SampleOp().make_operator()

    with pytest.raises(NotImplementedError):
        SampleDag(**_junk_dag_kwargs).make_dag()


def test_not_implemented3():
    class SampleOp(BaseOperatorFactory):
        pass

    with pytest.raises(NotImplementedError):
        SampleOp().make_operator()


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


def test_task_id_and_task_group():
    class SampleOp(OperatorFactory):
        i: int

        @property
        def default_task_id(self):
            return f"add_{self.i}"

        def _make_operators(self, *args, **kwargs) -> None:
            load_value = DummyOperator(task_id="load")
            compute_result = DummyOperator(task_id="compute")
            finish = DummyOperator(task_id="finish")

            load_value >> compute_result >> finish

    from data_dag.dag_factory import DagFactory

    class SampleDag(DagFactory):
        def _make_dag(self):
            SampleOp(i=3).make_operator()

    dag = SampleDag(**_junk_dag_kwargs).make_dag()
    load, compute, finish = dag.tasks
    assert load.task_id == "add_3.load"
    assert compute.task_id == "add_3.compute"
    assert finish.task_id == "add_3.finish"
