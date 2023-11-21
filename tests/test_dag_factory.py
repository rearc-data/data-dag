from datetime import datetime
from typing import List

import pytest
from airflow import DAG

try:
    from airflow.operators.empty import EmptyOperator
except ImportError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator

from data_dag.dag_factory import DagFactory
from data_dag.operator_factory import SimpleOperatorFactory


def test_simple():
    class EmptyOp(SimpleOperatorFactory):
        name: str

        def make_operator(self):
            return EmptyOperator(task_id=self.name)

    class SampleDag(DagFactory):
        empty_names: List[EmptyOp]

        def _make_dag(self):
            prev_op = None
            for empty in self.empty_names:
                next_op = empty.make_operator()
                if prev_op is not None:
                    prev_op >> next_op
                prev_op = next_op

    data = {
        "dag_id": "my_dag",
        "empty_names": ["start", "end"],
        "start_date": datetime.now(),
    }
    dag_factory = SampleDag.model_validate(data)
    dag = dag_factory.make_dag()
    assert isinstance(dag, DAG)
    assert dag.dag_id == "my_dag"
    op1, op2 = dag.topological_sort()
    assert isinstance(op1, EmptyOperator)
    assert op1.task_id == "start"
    assert isinstance(op2, EmptyOperator)
    assert op2.task_id == "end"


def test_failure_direct_instantiation():
    data = {"dag_id": "my_dag", "start_date": datetime.now()}
    pytest.raises(NotImplementedError, DagFactory.model_validate(data).make_dag)
