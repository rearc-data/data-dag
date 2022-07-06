import abc
from typing import List

import pytest

from data_dag.operator_factory import OperatorFactory
from data_dag.operator_factory.dynamic import DynamicOperatorFactory


def test_direct_dynamic_class():
    class ABase(DynamicOperatorFactory, abc.ABC):
        pass

    class A1(ABase):
        __type_name__ = "type1"
        x: str

    class A2(ABase):
        __type_name__ = "type2"
        x: str

    class A3(ABase):
        __type_name__ = "type3"
        x: str

    class Root(OperatorFactory):
        a: ABase

    assert ABase.__known_subclasses__ == {
        "type1": A1,
        "type2": A2,
        "type3": A3,
    }

    obj = {"a": {"type": "type2", "x": "yolo"}}
    obj = Root.parse_obj(obj)
    assert isinstance(obj, Root)
    obj1 = obj.a
    assert isinstance(obj1, A2)
    assert obj1.x == "yolo"


def test_indirect_dynamic_class():
    class ABase(DynamicOperatorFactory, abc.ABC):
        pass

    class A1(ABase):
        __type_name__ = "type1"
        x: str

    class A2(ABase):
        __type_name__ = "type2"
        x: str

    class A3(ABase):
        __type_name__ = "type3"
        x: str

    class Root(OperatorFactory):
        a: List[ABase]

    assert ABase.__known_subclasses__ == {
        "type1": A1,
        "type2": A2,
        "type3": A3,
    }

    obj = {
        "a": [
            {"type": "type2", "x": "yolo"},
            {"type": "type1", "x": "wassup"},
        ]
    }
    obj = Root.parse_obj(obj)
    assert isinstance(obj, Root)
    assert len(obj.a) == 2
    obj1, obj2 = obj.a
    assert isinstance(obj1, A2)
    assert obj1.x == "yolo"
    assert isinstance(obj2, A1)
    assert obj2.x == "wassup"


def test_default_dynamic_class():
    class ABase(DynamicOperatorFactory, abc.ABC):
        __default_type_name__ = "type2"

    class A1(ABase):
        __type_name__ = "type1"
        x: str

    class A2(ABase):
        __type_name__ = "type2"
        x: str

    a1 = ABase.parse_obj({"type": "type1", "x": "wassup"})
    assert isinstance(a1, A1)
    assert a1.x == "wassup"

    a2 = ABase.parse_obj({"type": "type2", "x": "lol"})
    assert isinstance(a2, A2)
    assert a2.x == "lol"

    ad = ABase.parse_obj({"x": "default thing"})
    assert isinstance(ad, A2)
    assert ad.x == "default thing"


def test_erroneous_dynamic_class():
    class ABase(DynamicOperatorFactory, abc.ABC):
        pass

    class A1(ABase):
        __type_name__ = "type1"
        x: str

    class A2(ABase):
        __type_name__ = "type2"
        x: str

    pytest.raises(TypeError, ABase.parse_obj, {"type": "typeNone", "x": "wassup"})


def test_failure_no_type_name():
    with pytest.warns(UserWarning):

        class ABase(DynamicOperatorFactory, abc.ABC):
            pass

        class _A1(ABase):
            x: str


def test_failure_no_type_given():
    class ABase(DynamicOperatorFactory, abc.ABC):
        pass

    class A1(ABase):
        __type_name__ = "type1"
        x: str

    with pytest.raises(TypeError):
        ABase.parse_obj({"x": "junk"})
