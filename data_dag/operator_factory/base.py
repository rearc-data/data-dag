import abc
from functools import cached_property
from typing import Union, Sequence, Optional

from airflow.models.taskmixin import TaskMixin
from pydantic import BaseModel

from data_dag.operator_factory._utils import _SimpleModelMixin


class OperatorFactory(BaseModel, TaskMixin, abc.ABC):
    def make_operator(self, *args, **kwargs) -> TaskMixin:
        raise NotImplementedError()

    # TODO: It would be nice to be able to use a factory just like an operator, with >> and << etc.
    #       However, that forces the factories into maintaining their operator instance, which is non-pydantic
    #       So for now, this doesn't quite fit into the Pydantic model, and it's probably a bad idea anyway...
    # _operator_instance: Optional[TaskMixin] = None
    # # @cached_property  # <-- this doesn't play well with Pydantic since it uses thread locks...
    # # the below is just explicitly not thread safe
    # @property
    # def operator(self) -> TaskMixin:
    #     if self._operator_instance is None:
    #         self._operator_instance = self.make_operator()
    #     return self._operator_instance
    #
    # @property
    # def roots(self):
    #     """Should return list of root operator List[BaseOperator]"""
    #     return self.operator.roots
    #
    # @property
    # def leaves(self):
    #     """Should return list of leaf operator List[BaseOperator]"""
    #     return self.operator.leaves
    #
    # def set_upstream(self, other: Union["TaskMixin", Sequence["TaskMixin"]]):
    #     """Set a task or a task list to be directly upstream from the current task."""
    #     self.operator.set_upstream(other)
    #
    # def set_downstream(self, other: Union["TaskMixin", Sequence["TaskMixin"]]):
    #     """Set a task or a task list to be directly downstream from the current task."""
    #     self.operator.set_downstream(other)
    #
    # def update_relative(self, other: "TaskMixin", upstream=True) -> None:
    #     """
    #     Update relationship information about another TaskMixin. Default is no-op.
    #     Override if necessary.
    #     """
    #     self.operator.update_relative(other, upstream)


class OperatorComponent(BaseModel, abc.ABC):
    """A non-operator component for use in other operator factories. Just a proxy for `pydantic.BaseModel`."""

    pass


class SimpleOperatorFactory(_SimpleModelMixin, OperatorFactory, abc.ABC):
    pass


class SimpleOperatorComponent(_SimpleModelMixin, OperatorComponent, abc.ABC):
    pass
