import abc
import inspect
from typing import Any, Type

from pydantic import BaseModel, model_validator
from pydantic.fields import FieldInfo
from typing_extensions import get_origin

from .base import OperatorComponent, OperatorFactory


def _dict_from_primitive(cls: "Type[_SimpleModelMixin]", obj):
    assert cls.__simple_field__ is not None and isinstance(
        cls.__simple_field__, FieldInfo
    )

    if not isinstance(obj, dict):
        return {cls.__simple_field__name__: obj}
    else:
        # Check if we're expecting a dictionary...
        tp = get_origin(cls.__simple_field__.annotation)
        if isinstance(tp, type) and issubclass(tp, dict):
            raise NotImplementedError(
                "Not yet sure how to handle sanitizing a dictionary when the class is just a proxy for a dictionary field"
            )
        ###
        return obj


class _SimpleModelMixin:
    """A mixin to support single-field pydantic models being parsed directly from primitives rather than requiring dictionaries"""

    @classmethod
    def __pydantic_init_subclass__(cls, **_kwargs):
        assert issubclass(cls, BaseModel)

        if not inspect.isabstract(cls) and abc.ABC not in cls.__bases__:
            required_fields = [
                (field_name, field)
                for field_name, field in cls.model_fields.items()
                if field.is_required()
            ]
            if len(required_fields) != 1:
                raise TypeError(
                    f"The non-abstract model {cls} must have exactly one non-default field (Found {required_fields} out of {cls.model_fields})"
                )

            field_name, field = required_fields[0]
            cls.__simple_field__name__ = field_name
            cls.__simple_field__ = field
            # cls.__pre_root_validators__ = [_dict_from_primitive]

    @model_validator(mode="before")
    @classmethod
    def _convert_to_dict_if_scalar(cls, data: Any) -> Any:
        return _dict_from_primitive(cls, data)

    @classmethod
    def model_validate(cls, data: Any):
        return super().model_validate(_dict_from_primitive(cls, data))


class SimpleOperatorFactory(_SimpleModelMixin, OperatorFactory, abc.ABC):
    """Identical to :py:class:`OperatorFactory` except that this represents predominantly a single field of metadata.

    The model that inherits from :py:class:`SimpleOperatorFactory` can only have a single non-required field
    (meaning no default value and not :py:class:`~typing.Optional`). In return the constructor for this object,
    in addition to being callable with a dictionary of field values, can also be called with a simple literal to fill
    in the single required field.

    Consider the following example::

        class FilePath(SimpleOperatorFactory):
            path: str  # <-- single required field
            is_file: bool = True  # <-- optional field (because of default)
            mime_type: str | None  # <-- optional field (because of Optional type)

            def make_operator(self):
                ...

    Normally, this object could only be instantiated using a dictionary::

        FilePath.model_validate({'path': 'path/to/file.txt'})

        # Or, in YAML:
        # outer_object:
        #   my_file:
        #     path: 'path/to/file.txt'
        # Or, in JSON:
        # {"outer_object": {"my_file": {"path": "path/to/file.txt"}}}

    However, because we inherit from :py:class:`SimpleOperatorFactory`, we can instantiate a ``FilePath`` by specifying just the ``path`` literal::

        FilePath.model_validate('path/to/file.txt')

        # Or, in YAML:
        # outer_object:
        #   my_file: 'path/to/file.txt'
        # Or, in JSON:
        # {"outer_object": {"my_file": "path/to/file.txt"}}
    """

    pass


class SimpleOperatorComponent(_SimpleModelMixin, OperatorComponent, abc.ABC):
    """An extension of :py:class:`OperatorComponent` to have the same single-field functionality as
    :py:class:`SimpleOperatorFactory`."""

    pass
