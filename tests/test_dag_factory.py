from datetime import datetime
from typing import List

import pytest
from airflow import DAG
from airflow.operators.dummy import DummyOperator

from data_dag.dag_factory import DagFactory
from data_dag.operator_factory import SimpleOperatorFactory


def test_simple():
    class DummyOp(SimpleOperatorFactory):
        name: str

        def make_operator(self):
            return DummyOperator(task_id=self.name)

    class SampleDag(DagFactory):
        dummy_names: List[DummyOp]

        def _make_dag(self):
            prev_op = None
            for dummy in self.dummy_names:
                next_op = dummy.make_operator()
                if prev_op is not None:
                    prev_op >> next_op
                prev_op = next_op

    data = {
        "dag_id": "my_dag",
        "dummy_names": ["start", "end"],
        "start_date": datetime.now(),
    }
    dag_factory = SampleDag.parse_obj(data)
    dag = dag_factory.make_dag()
    assert isinstance(dag, DAG)
    assert dag.dag_id == "my_dag"
    op1, op2 = dag.topological_sort()
    assert isinstance(op1, DummyOperator)
    assert op1.task_id == "start"
    assert isinstance(op2, DummyOperator)
    assert op2.task_id == "end"


def test_failure_direct_instantiation():
    data = {"dag_id": "my_dag", "start_date": datetime.now()}
    pytest.raises(NotImplementedError, DagFactory.parse_obj(data).make_dag)
