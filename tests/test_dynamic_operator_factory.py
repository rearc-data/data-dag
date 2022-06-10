import abc
from typing import List

from data_dag.operator_factory import DynamicOperatorFactory, OperatorFactory


def test_direct_dynamic_class():
    class ABase(DynamicOperatorFactory, abc.ABC):
        pass

    class A1(ABase):
        __type_name__ = 'type1'
        x: str

    class A2(ABase):
        __type_name__ = 'type2'
        x: str

    class A3(ABase):
        __type_name__ = 'type3'
        x: str

    class Root(OperatorFactory):
        a: ABase

    assert ABase.__known_subclasses__ == {
        'type1': A1,
        'type2': A2,
        'type3': A3,
    }

    obj = {
        'a': {
            'type': 'type2',
            'x': 'yolo'
        }
    }
    obj = Root.parse_obj(obj)
    assert isinstance(obj, Root)
    obj1 = obj.a
    assert isinstance(obj1, A2)
    assert obj1.x == 'yolo'


def test_indirect_dynamic_class():
    class ABase(DynamicOperatorFactory, abc.ABC):
        pass

    class A1(ABase):
        __type_name__ = 'type1'
        x: str

    class A2(ABase):
        __type_name__ = 'type2'
        x: str

    class A3(ABase):
        __type_name__ = 'type3'
        x: str

    class Root(OperatorFactory):
        a: List[ABase]

    assert ABase.__known_subclasses__ == {
        'type1': A1,
        'type2': A2,
        'type3': A3,
    }

    obj = {
        'a': [
            {
                'type': 'type2',
                'x': 'yolo'
            },
            {
                'type': 'type1',
                'x': 'wassup'
            },
        ]
    }
    obj = Root.parse_obj(obj)
    assert isinstance(obj, Root)
    assert len(obj.a) == 2
    obj1, obj2 = obj.a
    assert isinstance(obj1, A2)
    assert obj1.x == 'yolo'
    assert isinstance(obj2, A1)
    assert obj2.x == 'wassup'
