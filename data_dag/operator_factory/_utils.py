import abc
import inspect
from typing import Any, Type, get_origin

from pydantic import BaseModel


def _dict_from_primitive(cls: "Type[_SimpleModelMixin]", obj):
    assert cls.__simple_field__ is not None
    if not isinstance(obj, dict):
        return {cls.__simple_field__.name: obj}
    else:
        # Check if we're expecting a dictionary...
        tp = cls.__simple_field__.outer_type_
        tp = get_origin(tp) or tp
        if isinstance(tp, type) and issubclass(tp, dict):
            raise NotImplementedError(
                "Not yet sure how to handle sanitizing a dictionary when the class is just a proxy for a dictionary field"
            )
        ###
        return obj


class _SimpleModelMixin:
    """A mixin to support single-field pydantic models being parsed directly from primitives rather than requiring dictionaries"""

    def __init_subclass__(cls, **kwargs):
        assert issubclass(cls, BaseModel)

        if not inspect.isabstract(cls) and abc.ABC not in cls.__bases__:
            non_default_fields = [
                field
                for field in cls.__fields__.values()
                if field.default is None and field.default_factory is None
            ]
            if len(non_default_fields) != 1:
                raise TypeError(
                    f"A non-abstract inheritor of {cls} must have exactly one non-default field"
                )

            field = non_default_fields[0]
            cls.__simple_field__ = field
            cls.__pre_root_validators__ = [_dict_from_primitive]

    @classmethod
    def _enforce_dict_if_root(cls, obj: Any) -> Any:
        obj = super()._enforce_dict_if_root(obj)
        obj = _dict_from_primitive(cls, obj)
        return obj

    # @classmethod
    # def parse_obj(
    #     cls,
    #     obj: Any
    # ):
    #     return super().parse_obj(obj)
