from typing import Type, Dict

import dacite
from decorator import decorator
from pydantic.dataclasses import dataclass

from data_dag.utils import LazyDaciteConfig


class DagFactoryMixin:
    _dacite_config = LazyDaciteConfig(strict=True)

    @classmethod
    def _dacite_cast(cls):
        return []

    @classmethod
    def _dacite_type_hooks(cls):
        return {}

    @classmethod
    def _dacite_forward_references(cls):
        return {}

    def make_dag(self, *args, **kwargs):
        raise NotImplementedError()

    def from_dict(cls, dag: Dict):
        return dacite.from_dict(cls, dag, config=cls._dacite_config)


@decorator
def operator_factory(klass: Type):
    klass = dataclass(klass)
    klass = type(klass.__name__, (klass, DagFactoryMixin), {})
    return klass
