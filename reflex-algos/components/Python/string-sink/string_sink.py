from parallelm.components import ConnectableComponent
from parallelm.mlops import mlops


class StringSink(ConnectableComponent):

    def __init__(self, engine):
        super(self.__class__, self).__init__(engine)

    def _materialize(self, parent_data_objs, user_data):
        expected_str_value = self._params.get('expected-value', "default-string-value")
        actual_value = parent_data_objs[0]
        print("String Sink, Got:[{}] Expected: [{}] ".format(actual_value, expected_str_value))
        if expected_str_value != actual_value:
            raise Exception("Actual [{}] != Expected [{}]".format(actual_value, expected_str_value))
        check_test_mode = self._params.get('check-test-mode', False)
        if check_test_mode:
            if not mlops.test_mode:
                raise Exception("systemConfig '__test_mode__' has to be set to True in this test")
        return []
