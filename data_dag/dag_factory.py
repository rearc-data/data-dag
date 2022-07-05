from datetime import timedelta, datetime
from typing import Optional, Dict, List, Iterable, Union

from airflow.models.dag import (
    DagStateChangeCallback,
    ScheduleIntervalArgNotSet,
    ScheduleIntervalArg,
    DAG,
)
from airflow.timetables.base import Timetable
from pydantic import BaseModel


class DagFactory(BaseModel):
    dag_id: str
    description: Optional[str] = None
    schedule_interval: Optional[ScheduleIntervalArg] = None
    timetable: Optional[Timetable] = None
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
    on_success_callback: Optional[DagStateChangeCallback] = None
    on_failure_callback: Optional[DagStateChangeCallback] = None
    doc_md: Optional[str] = None
    params: Optional[Dict] = None
    access_control: Optional[Dict] = None
    is_paused_upon_creation: Optional[bool] = None
    jinja_environment_kwargs: Optional[Dict] = None
    render_template_as_native_obj: bool = False
    tags: Optional[List[str]] = None

    @property
    def dag_object(self):
        kwargs = {
            field: getattr(self, field, None)
            for field in self.fields
            if getattr(self, field, None) is not None
        }
        return DAG(**kwargs)

    def make_dag(self, *args, **kwargs):
        raise NotImplementedError()
