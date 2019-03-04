
import getpass


from parallelm.mlops import MLOps
from parallelm.mlops.singelton import Singleton
from parallelm.mlops.constants import Constants
from parallelm.mlops.mlops_rest_factory import MlOpsRestFactory
from parallelm.mlops.config_info import ConfigInfo
from parallelm.mlops.mlops_mode import MLOpsMode, OutputChannel


class MLOpsDevel(MLOps):

    def __init__(self):
        super(MLOpsDevel, self).__init__()

    def attach(self,
               ion_id,
               mlops_server=Constants.MLOPS_DEFAULT_HOST,
               mlops_port=Constants.MLOPS_DEFAULT_PORT,
               user=Constants.MLOPS_DEFAULT_USER,
               password=None):
        """
        Attach to a running ION and run in its context.
        Side effect: sets up mlops_context
        :param ion_id: the id of the ION to connect to
        :param mlops_server: the host to connect to
        :param mlops_port: the port MLOps is using
        :param user: user name to use for connection
        :param password: password to use for authentication, if None (default) then prompt for password
        :return:
    `
        Note: Attach only works for pure python code
        """
        self._logger.info("Connecting to mlops: {} {}: {} user: {} pass: {}".format(
            mlops_server, Constants.ION_LITERAL, ion_id, user, password))

        if password is None:
            password = getpass.getpass("Enter password:")

        # Connecting directly the server
        rest_helper = MlOpsRestFactory().get_rest_helper(MLOpsMode.ATTACH, mlops_server, mlops_port, None)
        token = rest_helper.login(user, password)

        # Setting the environment for mlops
        ci = ConfigInfo()
        ci.token = token
        ci.zk_host = None
        ci.mlops_port = str(mlops_port)  # Constants.MLOPS_DEFAULT_PORT
        ci.mlops_server = mlops_server
        ci.ion_id = ion_id
        ci.mlops_mode = MLOpsMode.ATTACH
        ci.output_channel_type = OutputChannel.PYTHON

        # TODO: for now assume node "0" - allow providing the node id or just become any node
        ci.ion_node_id = "0"
        ci.pipeline_id = "0"

        self._logger.info("MLOps configuration:\n{}".format(ci))
        ci.set_env()

        # calling init
        self.init(ctx=None, mlops_mode=MLOpsMode.ATTACH)


@Singleton
class MLOpsDevelSingleton(MLOpsDevel):
    pass


mlops_devel = MLOpsDevelSingleton.Instance()
