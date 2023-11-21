import abc
import inspect
from typing import Union

from pydantic import BaseModel, Field
from typing_extensions import Self

from .base import OperatorComponent, OperatorFactory


# With much help from
# https://stackoverflow.com/questions/23374715/changing-the-bases-of-an-object-based-on-arguments-to-init
class _DynamicOperatorBase:
    __type_kwarg_name__ = "type"
    __known_subclasses__ = None

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs):
        if not inspect.isabstract(cls) and abc.ABC not in cls.__bases__:
            if cls.__type_kwarg_name__ not in cls.model_fields:
                raise TypeError(
                    f"Dynamic subclass {cls} defined with no discriminator field, expected a literal field named `{cls.__type_kwarg_name__}`"
                )

            cls.__known_subclasses__.append(cls)

    @classmethod
    def discriminated_annotation(cls) -> type[Union]:
        return Union[tuple(cls.__known_subclasses__)]

    @classmethod
    def discrimated_field(cls, **kwargs) -> Field:
        return Field(discriminator=cls.__type_kwarg_name__, **kwargs)

    @classmethod
    def model_validate(cls, data: dict) -> Self:
        class PolymorphicWrapper(BaseModel):
            obj: cls.discriminated_annotation() = cls.discrimated_field()

        obj = PolymorphicWrapper.model_validate(dict(obj=data))
        return obj.obj


class DynamicOperatorFactory(_DynamicOperatorBase, OperatorFactory, abc.ABC):
    """An OperatorFactory that can automatically instantiate sub-classes based on the input data.

    Consider the following example::

        class InputFile(DynamicOperatorFactory, abc.ABC):
            pass

        class LocalFile(InputFile):
            type: Literal['local']

            path: str

        class S3File(InputFile):
            type: Literal['s3']

            bucket: str
            key: str

        TypeAdapter[InputFile.subtypes_annotation()](type='s3', bucket='my-bucket', key='my-key')
        # S3File(bucket='my-bucket', key='my-key')

    Note how the type of object that gets instantiated is dynamically chosen from the data, rather than specified by the code. This allows a supertype to be used in code, and for the subtype to be chosen at runtime based on data.

    To use a dynamic factory, define your base supertype to inherit directly from :py:class:`DynamicOperatorFactory` and :py:class:`abc.ABC`. The class can be totally empty, as in the example above. This top-level class will be populated with a list that will automatically track subclasses as they get define.

    .. warning::

        It's important to remember that, while subtypes are automatically tracked upon definition, they must still be imported somewhere. Make sure that when the supertype is imported, the subtypes also eventually get imported, or else they will be unavailable at DAG resolution time.

    Subclasses must define a `Literal`-annotated field with a name matching the value of ``__default_type_name__`` (`type` by default) in the top-level type.

    By default, the subclass is chosen by the ``"type"`` key in the input data. This can be changed by setting ``__type_kwarg_name__`` in the top-level type to some other string.

    This functionality is based on Pydantic's "discriminated union" feature. See that for more details.
    """

    def __init_subclass__(cls, **kwargs):
        if cls.__known_subclasses__ is None and cls is not DynamicOperatorFactory:
            cls.__known_subclasses__ = []

        super().__init_subclass__(**kwargs)


class DynamicOperatorComponent(_DynamicOperatorBase, OperatorComponent, abc.ABC):
    """Identical to :py:class:`DynamicOperatorFactory` but based on :py:class:`OperatorComponent` instead."""

    def __init_subclass__(cls, **kwargs):
        if cls.__known_subclasses__ is None and cls is not DynamicOperatorComponent:
            cls.__known_subclasses__ = []

        super().__init_subclass__(**kwargs)
