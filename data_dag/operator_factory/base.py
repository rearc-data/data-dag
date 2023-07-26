import abc
import contextlib
from typing import Optional, Sequence, Union

from airflow.models.taskmixin import TaskMixin
from airflow.utils.task_group import TaskGroup
from pydantic import BaseModel, Extra


class BaseOperatorFactory(BaseModel, abc.ABC):
    class Config:
        extra = Extra.forbid

    def make_operator(
        self, *args, **kwargs
    ) -> Union[TaskMixin, Sequence[TaskMixin], None]:
        """Converts this factory metadata into an operator.

        Returns:
            Zero or more operator-like things. The code that calls this should know how to handle the possible return types for this particular factory.
        """
        raise NotImplementedError()


class OperatorFactory(BaseOperatorFactory, abc.ABC):
    """An interface for writing operator factories."""

    task_id: Optional[str]

    @property
    def default_task_id(self) -> str:
        """If overridden, defines a default task ID when none is manually specified"""
        raise NotImplementedError()

    def get_task_id(self):
        """Provides the custom task ID if provided, else this factory's default task ID"""
        return self.task_id or self.default_task_id

    @contextlib.contextmanager
    def _task_group(self):
        with TaskGroup(group_id=self.get_task_id()) as group:
            yield

        return group

    def make_operator(
        self, *args, **kwargs
    ) -> Union[TaskMixin, Sequence[TaskMixin], None]:
        """Converts this factory metadata into an operator.

        If you need to create multiple operators, whether connected or not, implement :py:meth:`_make_operators`
        instead, and they will be automatically wrapped in a :py:class:`airflow.utils.task_group.TaskGroup`.

        Returns:
            Zero or more operator-like things. The code that calls this should know how to handle the possible return types for this particular factory.
        """
        with self._task_group() as group:
            self._make_operators(*args, **kwargs)

        return group

    def _make_operators(self, *args, **kwargs) -> None:
        """Can be implemented instead of :py:meth:`make_operator` to define an operator collection inside a
        default :py:class:`airflow.utils.task_group.TaskGroup`"""
        raise NotImplementedError()


class OperatorComponent(BaseModel, abc.ABC):
    """A non-operator component for use in other operator factories. Just a proxy for :py:class:`pydantic.BaseModel`."""

    class Config:
        extra = Extra.forbid
