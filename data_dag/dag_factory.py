import abc
from datetime import timedelta, datetime
from typing import Optional, Dict, List, Iterable, Union

from airflow.models.dag import DAG
from pydantic import BaseModel


class DagFactory(BaseModel, abc.ABC):
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

    @property
    def default_dag_kwargs(self) -> Dict:
        return {}

    def make_dag_object(self, **overrides) -> DAG:
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
        dag = self.make_dag_object(**(dag_overrides or {}))
        with dag:
            self._make_dag(*args, **kwargs)

        return dag

    def _make_dag(self, *args, **kwargs) -> None:
        raise NotImplementedError()
