"""
Factory to return correct implementation of MlopsRestHelper.
"""

from parallelm.mlops.constants import Constants
from parallelm.mlops.base_obj import BaseObj
from parallelm.mlops.mlops_rest_connected import MlOpsRestConnected
from parallelm.mlops.mlops_rest_standalone import MlOpsRestStandAlone

from parallelm.mlops.mlops_mode import MLOpsMode

class MlOpsRestFactory(BaseObj):
    def __init__(self):
        super(MlOpsRestFactory, self).__init__(__name__)

    def get_rest_helper(self, mode, mlops_server=Constants.MLOPS_DEFAULT_HOST, mlops_port=Constants.MLOPS_DEFAULT_PORT,
                        token=None):
        if mode == MLOpsMode.STAND_ALONE:
            return MlOpsRestStandAlone()
        return MlOpsRestConnected(mlops_server, mlops_port, token)