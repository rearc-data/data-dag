import abc
from typing import List, Literal

import pytest
from pydantic import ValidationError

from data_dag.operator_factory import OperatorFactory
from data_dag.operator_factory.dynamic import (
    DynamicOperatorComponent,
    DynamicOperatorFactory,
)


def test_direct_dynamic_class():
    class ABase(DynamicOperatorFactory, abc.ABC):
        pass

    class A1(ABase):
        type: Literal["type1"] = "type1"
        x: str

    class A2(ABase):
        type: Literal["type2"] = "type2"
        x: str

    class A3(ABase):
        type: Literal["type3"] = "type3"
        x: str

    class Root(OperatorFactory):
        a: ABase.discriminated_annotation() = ABase.discrimated_field()

    assert ABase.__known_subclasses__ == [A1, A2, A3]

    obj = {"a": {"type": "type2", "x": "yolo"}}
    obj = Root.model_validate(obj)
    assert isinstance(obj, Root)
    obj1 = obj.a
    assert isinstance(obj1, A2)
    assert obj1.x == "yolo"


def test_indirect_dynamic_class():
    class ABase(DynamicOperatorFactory, abc.ABC):
        pass

    class A1(ABase):
        type: Literal["type1"] = "type1"
        x: str

    class A2(ABase):
        type: Literal["type2"] = "type2"
        x: str

    class A3(ABase):
        type: Literal["type3"] = "type3"
        x: str

    class Root(OperatorFactory):
        a: List[ABase.discriminated_annotation()] = ABase.discrimated_field()

    assert set(ABase.__known_subclasses__) == {A1, A2, A3}

    obj = {
        "a": [
            {"type": "type2", "x": "yolo"},
            {"type": "type1", "x": "wassup"},
        ]
    }
    obj = Root.model_validate(obj)
    assert isinstance(obj, Root)
    assert len(obj.a) == 2
    obj1, obj2 = obj.a
    assert isinstance(obj1, A2)
    assert obj1.x == "yolo"
    assert isinstance(obj2, A1)
    assert obj2.x == "wassup"

    assert ABase.model_validate(dict(type="type1", x="yolo")) == A1(x="yolo")


def test_failure_erroneous_dynamic_class():
    class ABase(DynamicOperatorFactory, abc.ABC):
        pass

    class A1(ABase):
        type: Literal["type1"] = "type1"
        x: str

    class A2(ABase):
        type: Literal["type2"] = "type2"
        x: str

    pytest.raises(
        ValidationError, ABase.model_validate, {"type": "typeNone", "x": "wassup"}
    )


def test_failure_no_type_name():
    with pytest.raises(TypeError):

        class ABase(DynamicOperatorFactory, abc.ABC):
            pass

        class _A1(ABase):
            x: str


def test_failure_no_type_given():
    class ABase(DynamicOperatorFactory, abc.ABC):
        pass

    class A1(ABase):
        type: Literal["type1"] = "type1"
        x: str

    with pytest.raises(ValidationError):
        ABase.model_validate({"x": "junk"})


def test_failure_both_specified_and_explicit_type():
    class ABase(DynamicOperatorFactory, abc.ABC):
        pass

    class A1(ABase):
        type: Literal["type1"] = "type1"
        x: str

    with pytest.raises(ValidationError):
        A1.model_validate({"type": "type2", "x": "junk"})


def test_customize_type_name():
    class ABase(DynamicOperatorFactory, abc.ABC):
        __type_kwarg_name__ = "different_type"

    class A1(ABase):
        different_type: Literal["type1"] = "type1"
        x: str

    class A2(ABase):
        different_type: Literal["type2"] = "type2"
        y: str

    class BBase(DynamicOperatorFactory, abc.ABC):
        pass

    class B1(BBase):
        type: Literal["type1"] = "type1"
        a: str

    class B2(BBase):
        type: Literal["type2"] = "type2"
        b: str

    assert set(ABase.__known_subclasses__) == {A1, A2}
    assert set(BBase.__known_subclasses__) == {B1, B2}

    a1 = ABase.model_validate({"different_type": "type1", "x": "lol"})
    assert isinstance(a1, A1)
    assert a1.x == "lol"

    b2 = BBase.model_validate({"type": "type2", "b": "yup"})
    assert isinstance(b2, B2)
    assert b2.b == "yup"


def test_basic_dynamic_component():
    class Uri(DynamicOperatorComponent, abc.ABC):
        pass

    class S3Uri(Uri):
        type: Literal["s3"] = "s3"
        bucket: str
        key: str

    uri = Uri.model_validate(dict(type="s3", bucket="my-bucket", key="my-key"))
    assert uri == S3Uri(bucket="my-bucket", key="my-key")
