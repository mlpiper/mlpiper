from parallelm.pipeline.component_runner.component_runner import ComponentRunner


class PythonConnectedComponentRunner(ComponentRunner):

    def __init__(self, ml_engine, dag_node):
        super(PythonConnectedComponentRunner, self).__init__(ml_engine, dag_node)

    def configure(self, params):
        self._params = params
        self._dag_node.main_cls().configure(params)

    def run(self, parent_data_objs):
        self._logger.info("running python connected component")
        data_objs = self._dag_node.main_cls().materialize(parent_data_objs)
        return data_objs
