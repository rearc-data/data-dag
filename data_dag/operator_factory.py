import abc
import inspect
import warnings

from pydantic import BaseModel
from pydantic.main import ModelMetaclass


class OperatorFactory(BaseModel, abc.ABC):
    def make_operator(self, *args, **kwargs):
        raise NotImplementedError()


# With much help from https://stackoverflow.com/questions/23374715/changing-the-bases-of-an-object-based-on-arguments-to-init
class _DynamicModelMetaclass(ModelMetaclass):
    def __new__(mcs, *args, **kwargs):
        cls = super(_DynamicModelMetaclass, mcs).__new__(mcs, *args, **kwargs)
        attr_name = cls.__type_attr_name__
        assert attr_name.startswith('__'), (cls, attr_name)
        cls.__known_subclasses__ = dict()
        return cls

    def __call__(cls, *args, **kwargs):
        kwarg_name = cls.__type_kwarg_name__
        try:
            subtype = kwargs.pop(kwarg_name)
        except KeyError as ex:
            if cls.__default_type_name__ is not None:
                subtype = cls.__default_type_name__
            else:
                raise TypeError(f"Failed to find type kwarg `{kwarg_name}` while instantiating {cls}") from ex
        try:
            specified_cls = cls.__known_subclasses__[subtype]
        except KeyError as ex:
            raise TypeError(f"Subtype `{subtype}` not found for {cls}. Options are {list(cls.__known_subclasses__)}") from ex

        print((specified_cls, args, kwargs))
        return super(_DynamicModelMetaclass, specified_cls).__call__(*args, **kwargs)


class DynamicOperatorFactory(OperatorFactory, abc.ABC, metaclass=_DynamicModelMetaclass):
    __type_name__ = None
    __type_attr_name__ = '__type_name__'
    __default_type_name__ = None
    __type_kwarg_name__ = 'type'

    def __init_subclass__(cls, **kwargs):
        if not inspect.isabstract(cls) and abc.ABC not in cls.__bases__:
            try:
                subtype_name = cls.__type_name__
            except (AttributeError, AssertionError) as ex:
                raise TypeError(
                    f"Must specify a subtype name {cls.__type_attr_name__} for non-abstract subclass {cls}") from ex

            if not subtype_name:
                warnings.warn(f"Type {cls} does not specify a subtype name (as __type_name__) and cannot be dynamically instantiated; if this is intentional, make the clas abstract, like `class {cls.__name__}(..., abc.ABC):`")

            cls.__known_subclasses__[subtype_name] = cls
