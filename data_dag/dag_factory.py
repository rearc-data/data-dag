import abc
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional, Union

from airflow.models.dag import DAG
from pydantic import BaseModel, Extra


class DagFactory(BaseModel, abc.ABC):
    """
    Base factory class for generating DAGs.

    This class serves as a metadata wrapper around :py:class:`airflow.models.dag.DAG`.

    The simplest way to create a DAG factory is to inherit from :py:class:`DagFactory` and implement :py:meth:`_make_dag`, like::

        class MyKindOfDag(DagFactory):
            def _make_dag(self):
                start = DummyOperator(...)
                do_something = PythonOperator(...)
                end = DummyOperator(...)

                start >> do_something >> end

    The :py:class:`~airflow.models.dag.DAG` object itself will be automatically created and opened prior to :py:meth:`_make_dag` being called.
    The final DAG can be obtained by calling :py:meth:`make_dag` on an instance of your factory::

        my_particular_dag_metadata = MyKindOfDag()
        dag = my_particular_dag_metadata.make_dag()

    For documentation on the attributes of :py:class:`DagFactory`, see the `DAG type docs <https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html#airflow.models.dag.DAG>`_.

    DAG keyword arguments can be passed in any of three ways (in ascending priority):
    1. Overriding :py:meth:`default_dag_kwargs` to return a dictionary of default keyword arguments
    2. Passing in data during construction (either directly or in an overridden constructor)
    3. As a ``dag_overrides`` dictionary when calling :py:meth:`make_dag`
    """

    dag_id: str
    description: Optional[str] = None
    schedule_interval: Union[timedelta, str, None] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    full_filepath: Optional[str] = None
    template_searchpath: Optional[Union[str, Iterable[str]]] = None
    user_defined_macros: Optional[Dict] = None
    user_defined_filters: Optional[Dict] = None
    default_args: Optional[Dict] = None
    concurrency: Optional[int] = None
    max_active_tasks: Optional[int] = None
    max_active_runs: Optional[int] = None
    dagrun_timeout: Optional[timedelta] = None
    default_view: Optional[str] = None
    orientation: Optional[str] = None
    catchup: Optional[bool] = None
    doc_md: Optional[str] = None
    params: Optional[Dict] = None
    access_control: Optional[Dict] = None
    is_paused_upon_creation: Optional[bool] = None
    jinja_environment_kwargs: Optional[Dict] = None
    render_template_as_native_obj: bool = False
    tags: Optional[List[str]] = None

    class Config:
        extra = Extra.forbid

    @property
    def default_dag_kwargs(self) -> Dict:
        """Override this property in a subclass to provide default arguments to the
        :py:class:`airflow.models.dag.DAG` constructor."""
        return {}

    def make_dag_object(self, **overrides) -> DAG:
        """Creates the basic :py:class:`airflow.models.dag.DAG` object represented by this metadata.

        This doesn't populate the DAG with nodes, it is only responsible for creating the initial DAG object.
        """
        kwargs = self.default_dag_kwargs.copy()
        kwargs.update(
            {
                field: getattr(self, field, None)
                for field in DagFactory.__fields__
                if getattr(self, field, None) is not None
            }
        )
        kwargs.update(overrides)
        return DAG(**kwargs)

    def make_dag(self, *args, dag_overrides: Optional[Dict] = None, **kwargs) -> DAG:
        """Creates and populates a :py:class:`airflow.models.dag.DAG` represented by this metadata"""
        dag = self.make_dag_object(**(dag_overrides or {}))
        with dag:
            self._make_dag(*args, **kwargs)

        return dag

    def _make_dag(self, *args, **kwargs) -> None:
        """Override this method in a subclass to populate the :py:class:`airflow.models.dag.DAG` object with nodes and edges"""
        raise NotImplementedError()
