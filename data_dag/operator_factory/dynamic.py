import abc
import inspect
import warnings

from pydantic.main import ModelMetaclass

from .base import OperatorComponent, OperatorFactory


class _DynamicModelMetaclass(ModelMetaclass):
    def __new__(mcs, *args, **kwargs):
        cls = super().__new__(mcs, *args, **kwargs)
        cls.__known_subclasses__ = dict()
        return cls

    def __call__(cls, *args, **kwargs):
        known_subtype = cls.__type_name__
        specified_subtype = kwargs.pop(
            cls.__type_kwarg_name__, cls.__default_type_name__
        )

        if known_subtype is None and specified_subtype is None:
            raise TypeError(
                f"Failed to find type kwarg `{cls.__type_kwarg_name__}` while instantiating {cls}"
            )
        elif known_subtype is not None and specified_subtype is not None:
            raise TypeError(
                f"Cannot specify explicit `{cls.__type_kwarg_name__}` to specific type {cls}"
            )

        # At this point, we know that exactly one of known_subtype and specified_subtype is given
        assert bool(known_subtype) ^ bool(specified_subtype), (
            known_subtype,
            specified_subtype,
        )

        if known_subtype:
            specified_cls = cls
        elif specified_subtype:
            try:
                specified_cls = cls.__known_subclasses__[specified_subtype]
            except KeyError as ex:
                raise TypeError(
                    f"Subtype `{specified_subtype}` not found for {cls}. Options are {list(cls.__known_subclasses__)}"
                ) from ex
        else:  # pragma: no cover
            assert False, ("How did we get here?", known_subtype, specified_subtype)

        return super(_DynamicModelMetaclass, specified_cls).__call__(*args, **kwargs)


# With much help from
# https://stackoverflow.com/questions/23374715/changing-the-bases-of-an-object-based-on-arguments-to-init
class _DynamicOperatorBase:
    __type_name__ = None
    __default_type_name__ = None
    __type_kwarg_name__ = "type"

    def __init_subclass__(cls, **kwargs):
        if not inspect.isabstract(cls) and abc.ABC not in cls.__bases__:
            subtype_name = cls.__type_name__

            if not subtype_name:
                warnings.warn(
                    f"Type {cls} does not specify a subtype name (as __type_name__) and cannot be dynamically instantiated; if this is intentional, make the clas abstract, like `class {cls.__name__}(..., abc.ABC):`"
                )

            cls.__known_subclasses__[subtype_name] = cls


class DynamicOperatorFactory(
    OperatorFactory, _DynamicOperatorBase, abc.ABC, metaclass=_DynamicModelMetaclass
):
    """An OperatorFactory that can automatically instantiate sub-classes based on the input data.

    Consider the following example::

        class InputFile(DynamicOperatorFactory, abc.ABC):
            pass

        class LocalFile(InputFile):
            __type_name__ = 'local'

            path: str

        class S3File(InputFile):
            __type_name__ = 's3'

            bucket: str
            key: str

        InputFile.parse_obj({'type': 's3', 'bucket': 'my-bucket', 'key': 'my-key'})
        # S3File(bucket='my-bucket', key='my-key')

    Note how the type of object that gets instantiated is dynamically chosen from the data, rather than specified by the code. This allows a supertype to be used in code, and for the subtype to be chosen at runtime based on data.

    To use a dynamic factory, define your base supertype to inherit directly from :py:class:`DynamicOperatorFactory` and :py:class:`abc.ABC`. The class can be totally empty, as in the example above. This top-level class will be populated with a dictionary that will automatically track subclasses as they get define.

    .. warning::

        It's important to remember that, while subtypes are automatically tracked upon definition, they must still be imported somewhere. Make sure that when the supertype is imported, the subtypes also eventually get imported, or else they will be unavailable at DAG resolution time.

    Subclasses must either define ``__type_name__ = "some_name"`` or else inherit from :py:class:`abc.ABC` to indicate that they are abstract. Classes that are not abstract and not named will generate a warning.

    A default subtype can be specified using ``__default_type_name__`` in the top-level type. Note that this is the ``__type_name__`` of the default subclass, not the class name itself.

    By default, the subclass is chosen by the ``"type"`` key in the input data. This can be changed by setting ``__type_kwarg_name__`` in the top-level type to some other string. This key will be stripped from the input data and all other keys will be passed along to the subtype's constructor without further modification.

    Attempting to construct a top-level object, either directly (with its constructor) or using ``parse_obj``, without specifying a "type" (or whatever you renamed the key to be) will result in a :py:exc:`TypeError`.

    .. note::

        Pydantic already supports Union types, so why would we use a custom DynamicOperatorFactory instead?

        Dynamic factories provide two key advantages:

        - The subtype selected is explicit rather than implicit. The subtypes don't need to be distinguishable in any other way besides their ``__type_name__``, nor is there any kind of ordering of the subtypes.
        - The list of options is automatically maintained, as long as the modules containing the subtypes are sure to be imported. That is, another component or factory can use the top-level type to annotate one of its fields, and the subtypes will automatically be implied.
    """


class DynamicOperatorComponent(
    OperatorComponent, _DynamicOperatorBase, abc.ABC, metaclass=_DynamicModelMetaclass
):
    """Identical to :py:class:`DynamicOperatorFactory` but based on :py:class:`OperatorComponent` instead."""
