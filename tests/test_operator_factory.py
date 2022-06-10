from data_dag.operator_factory import OperatorFactory


def test1():
    class SampleOp(OperatorFactory):
        to_add: float

        def make_operator(self, *args, **kwargs):
            return 3 + self.to_add

    data = {'to_add': 5}
    op = SampleOp.parse_obj(data)
    assert op.make_operator() == 8
