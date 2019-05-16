from parallelm.components import ConnectableComponent
from parallelm.mlops import mlops


class StringSource(ConnectableComponent):

    def __init__(self, engine):
        super(self.__class__, self).__init__(engine)

    def _materialize(self, parent_data_objs, user_data):
        str_value = self._params.get('value', "default-string-value")
        check_test_mode = self._params.get('check-test-mode', False)
        if check_test_mode:
            if not mlops.test_mode:
                raise Exception("systemConfig '__test_mode__' has to be set to True in this test")
        return [str_value]
