import abc
import inspect
import warnings

from pydantic.main import ModelMetaclass

from data_dag.operator_factory import OperatorFactory


class _DynamicModelMetaclass(ModelMetaclass):
    def __new__(mcs, *args, **kwargs):
        cls = super(_DynamicModelMetaclass, mcs).__new__(mcs, *args, **kwargs)
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
class DynamicOperatorFactory(
    OperatorFactory, abc.ABC, metaclass=_DynamicModelMetaclass
):
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
