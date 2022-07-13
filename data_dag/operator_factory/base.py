import abc
from typing import Union, Sequence

from airflow.models.taskmixin import TaskMixin
from pydantic import BaseModel


class OperatorFactory(BaseModel, abc.ABC):
    """An interface for writing operator factories."""

    def make_operator(
        self, *args, **kwargs
    ) -> Union[TaskMixin, Sequence[TaskMixin], None]:
        """Should generate zero or more operator-like things.

        To wrap a pattern of operators into a single operator-like object, use :py:class:`airflow.utils.task_group.TaskGroup`.

        Returns:
            Zero or more operator-like things. The code that calls this should know how to handle the possible return types for this particular factory.
        """
        raise NotImplementedError()


class OperatorComponent(BaseModel, abc.ABC):
    """A non-operator component for use in other operator factories. Just a proxy for :py:class:`pydantic.BaseModel`."""

    pass
