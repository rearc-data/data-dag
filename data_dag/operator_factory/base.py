import abc
from typing import Union, Sequence

from airflow.models.taskmixin import TaskMixin
from pydantic import BaseModel

from data_dag.operator_factory._utils import _SimpleModelMixin


class OperatorFactory(BaseModel, abc.ABC):
    def make_operator(
        self, *args, **kwargs
    ) -> Union[TaskMixin, Sequence[TaskMixin], None]:
        raise NotImplementedError()


class OperatorComponent(BaseModel, abc.ABC):
    """A non-operator component for use in other operator factories. Just a proxy for `pydantic.BaseModel`."""

    pass


class SimpleOperatorFactory(_SimpleModelMixin, OperatorFactory, abc.ABC):
    pass


class SimpleOperatorComponent(_SimpleModelMixin, OperatorComponent, abc.ABC):
    pass
