import abc
import contextlib
from typing import Union, Sequence, Optional

from airflow.models.taskmixin import TaskMixin
from airflow.utils.task_group import TaskGroup
from pydantic import BaseModel


class OperatorFactory(BaseModel, abc.ABC):
    """An interface for writing operator factories."""

    task_id: Optional[str]

    @property
    def default_task_id(self) -> str:
        raise NotImplementedError()

    def get_task_id(self):
        return self.task_id or self.default_task_id

    @contextlib.contextmanager
    def _task_group(self):
        with TaskGroup(group_id=self.get_task_id()) as group:
            yield

        return group

    def make_operator(
        self, *args, **kwargs
    ) -> Union[TaskMixin, Sequence[TaskMixin], None]:
        """Should generate zero or more operator-like things.

        To wrap a pattern of operators into a single operator-like object, use :py:class:`airflow.utils.task_group.TaskGroup`.

        Returns:
            Zero or more operator-like things. The code that calls this should know how to handle the possible return types for this particular factory.
        """
        with self._task_group() as group:
            self._make_operators(*args, **kwargs)

        return group

    def _make_operators(self, *args, **kwargs) -> None:
        """Can be implemented instead of :py:meth:`make_operator` to define an operator collection inside a default TaskGroup"""
        raise NotImplementedError()


class OperatorComponent(BaseModel, abc.ABC):
    """A non-operator component for use in other operator factories. Just a proxy for :py:class:`pydantic.BaseModel`."""

    pass
