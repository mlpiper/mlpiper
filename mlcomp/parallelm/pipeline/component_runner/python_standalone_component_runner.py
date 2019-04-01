import runpy
import sys
import os

from parallelm.pipeline.pipeline_utils import main_component_module
from parallelm.pipeline.component_runner.standalone_component_runner import StandaloneComponentRunner
from parallelm.pipeline.pipeline_utils import assemble_cmdline_from_args
from parallelm.pipeline.component_language import ComponentLanguage
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert import HTMLExporter

mlops_loaded = False
try:
    from parallelm.mlops import mlops
    from parallelm.mlops.stats.opaque import Opaque
    from parallelm.mlops.stats.html import HTML
    mlops_loaded = True
except ImportError as e:
    print("Note: was not able to import mlops: " + str(e))
    pass  # Designed for tests


class PythonStandaloneComponentRunner(StandaloneComponentRunner):

    def __init__(self, ml_engine, dag_node):
        super(PythonStandaloneComponentRunner, self).__init__(ml_engine, dag_node)

    def run(self, parent_data_objs):
        self._logger.info("Running stand alone python component")

        cmdline = assemble_cmdline_from_args(self._params)
        self._logger.debug("cmdline: {}".format(cmdline))

        orig_cmdline = sys.argv[1:]
        sys.argv[1:] = cmdline

        module_main_prog = main_component_module(self._dag_node.comp_desc())
        self._logger.info("Main program: {}".format(module_main_prog))
        self._logger.info("Dag Node {}".format(self._dag_node))

        try:
            if self._dag_node.comp_language() == ComponentLanguage.PYTHON:
                # Running the module as a script, using the __main__ as the run_name
                runpy.run_module(str(module_main_prog), run_name="__main__", alter_sys=True)
            elif self._dag_node.comp_language() == ComponentLanguage.JUPYTER:
                # run the jupyter notebook
                self._logger.info("Jupyter Notebook runner")
                comp_dir = self._dag_node.comp_root_path()
                self._logger.debug("Component directory {} -> {}".format(comp_dir, os.listdir(comp_dir)))

                notebook_filename = os.path.join(comp_dir, self._dag_node.comp_program())
                with open(notebook_filename) as f:
                    nb = nbformat.read(f, as_version=4)
                # TODO this must come from EE - the type of kernel needed
                ep = ExecutePreprocessor(timeout=600, kernel_name='python2')
                ep.preprocess(nb, {'metadata': {'path': comp_dir}})
                html_exporter = HTMLExporter()

                # 3. Process the notebook we loaded earlier
                (body, resources) = html_exporter.from_notebook_node(nb)
                if mlops_loaded:
                    html = HTML().name("Jupyter Output").data(body)
                    mlops.set_stat(html)
                else:
                    self._logger.error("Jupyter: mlops not loaded, so no output available")
        finally:
            sys.argv[1:] = orig_cmdline
