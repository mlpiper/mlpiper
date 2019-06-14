"""
MLOps helper class to fetch objects from the ECO server using REST interfaces.
"""
import abc
from future.utils import with_metaclass

from parallelm.mlops.base_obj import BaseObj


class MlOpsRestHelper(with_metaclass(abc.ABCMeta, BaseObj)):
    """
    This class is a set of helpers to fetch configuration information and models from the MLOps server.
    """

    def __init__(self, mlops_server='localhost', mlops_port=3456, token=None):
        super(MlOpsRestHelper, self).__init__(__name__)

        self._mlops_server = mlops_server
        self._mlops_port = mlops_port
        self._token = token
        self._prefix = None

    def set_prefix(self, prefix):
        """
        Set the prefix used by the MLOPs REST endpoint
        :param prefix: REST endpoint prefix
        """
        self._prefix = prefix
        return self

    @abc.abstractmethod
    def login(self, user='admin', auth='admin'):
        """
        This should be the only ECO REST API call not requiring a security token. Instead, use the account username
        and password for an authorized MLOps user.
        Command: curl -H "Content-Type: application/json"
        -X POST -d '{"username":"admin","password":"admin"}'
        http://hostname:3456/auth/login

        On success, sets the security token internally.
        :param user: account user name
        :param auth: account user password
        :raises Exceptions for connection or authentication errors
        """
        pass

    @abc.abstractmethod
    def get_model_list(self):
        """
        Requests the list of models from MLOps
        :return: the list of models
        """
        pass

    @abc.abstractmethod
    def post_model_as_file(self, model):
        """
        Posts a file to the server
        :param model: :class:`Model` object to publish
        :return: model_id
        """
        pass

    @abc.abstractmethod
    def get_groups(self):
        """
        Requests the list of groups from MLOps
        :return: the list of groups
        """
        pass

    @abc.abstractmethod
    def get_agents(self):
        """
        Requests the list of agents from MLOps
        :return: the list of agents
        """
        pass

    @abc.abstractmethod
    def get_workflow_instance(self, ion_instance_id):
        """
        Requests the workflow instance for the given ION
        :param ion_instance_id: the ION to get the workflow instance of
        :return: the workflow instance
        """
        pass

    @abc.abstractmethod
    def get_health_thresholds(self, ion_instance_id):
        pass

    @abc.abstractmethod
    def download_model(self, model_id):
        """
        Requests the model corresponding to the given model_id
        :param model_id: ID of the model
        :return: the model
        :raises MLOpsException
        """
        pass

    @abc.abstractmethod
    def get_stat(self, stat_name, ion_id, workflow_node_id, agent_id, pipeline_id, start_time, end_time):
        """
        Get the given statistic from MLOps server
        :param stat_name: name of the desired statistic
        :param ion_id: ION
        :param workflow_node_id: which node of the ION generated the statistic
        :param agent_id: which agent of the ION generated the statistic
        :param pipeline_id: which pipeline of the iON generated the statistic
        :param start_time: only return statistics at or after this time
        :param end_time: only return statistics at or before this time
        :return: the value of the statistic
        :raises MLOpsException
        """
        # TODO Add try-except
        pass

    @abc.abstractmethod
    def get_model_stats(self, model_id):
        pass

    @abc.abstractmethod
    def get_alerts(self, query_args):
        pass

    @abc.abstractmethod
    def done(self):
        pass

    @abc.abstractmethod
    def get_uuid(self, type):
        pass
