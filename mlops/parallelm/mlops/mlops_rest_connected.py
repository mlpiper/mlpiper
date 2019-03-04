"""
Implementation of REST client interfaces when in connected or attached mode.
"""
try:  # python3
    from urllib.parse import urlencode
    from urllib.parse import quote as encoder
except ImportError:  # python2
    from urllib import urlencode
    from urllib import quote as encoder

import requests
import json
import time

from parallelm.mlops.mlops_rest_interfaces import MlOpsRestHelper
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.constants import Constants, MLOpsRestHandles
from parallelm.mlops.constants import HTTPStatus
from parallelm.mlops.models.model import ModelMetadata


def build_url(host, port=None, *res, **params):
    """
    Function to build url. Examples:
    build_url('localhost','3456', 'agents') => http://localhost:3456/agents
    build_url('localhost','3456', 'agents', startTime=12345678) => http://localhost:3456/agents?startTime=12345678
    :param host: REST service host
    :param port: REST service port
    :param params: REST request parameters
    """
    base_url = Constants.SERVER_PROTOCOL + Constants.URL_SCHEME + Constants.URL_HIER_PART
    base_url += host

    if port:
        base_url += Constants.URL_SCHEME + str(port)

    url = base_url
    for r in res:
        if r is not None:
            url = '{}/{}'.format(url, r)
    if params:
        url = '{}?{}'.format(url, urlencode(params))

    return url


class MlOpsRestConnected(MlOpsRestHelper):
    """
    This class is a set of helpers to fetch configuration information and models from the MLOps server.
    """

    def __init__(self, mlops_server=Constants.MLOPS_DEFAULT_HOST, mlops_port=Constants.MLOPS_DEFAULT_PORT, token=None):
        super(MlOpsRestConnected, self).__init__(__name__)

        self._mlops_server = mlops_server
        self._mlops_port = mlops_port
        self._token = token
        self._prefix = None
        self._service_unavail_sleep_time = 5

    def _return_cookie(self):
        return {'token': self._token}

    def _get_url_request_response_as_json(self, url):
        """
        TODO: The better way to detect proper(JSON) content would be to check Content-Type header first.
              But ECO sends much of the data as application/octet-stream type.
              So if we want to use HTTP headers to understand what data is being send, we should first
              define our own guidelines about actual formats we send and types we set in headers.
        """
        ret = ""
        response = self._get_url_request_response(url)
        try:
            ret = response.json()
        except Exception as e:
            raise MLOpsException("Trying to parse response content as json:\n "
                                 "Content(trimmed): {}\n"
                                 "Content-Type: {}\n"
                                 "failed with error: {}"
                                 .format(str(response.text)[:2048], response.headers['Content-Type'], str(e)))
        return ret

    def _get_url_request_response(self, url):

        counter = 0
        while True:

            r = requests.get(url, cookies=self._return_cookie())
            if r.status_code == HTTPStatus.OK:
                return r
            elif r.status_code == HTTPStatus.SERVICE_UNAVAIL:
                self._warn("{} Got {} from server - possibly server is down - will try again in 5 seconds, url: {}".format(
                    counter, r.status_code, url))
                counter += 1
                time.sleep(self._service_unavail_sleep_time)
            else:
                raise MLOpsException("Got HTTP Error [{}]. GET url [{}]. error [{}]".format(r.status_code, url, r.text))

    def login(self, user='admin', auth='admin'):
        payload = {"username": user, "password": auth}
        url = build_url(self._mlops_server, self._mlops_port, self._prefix, MLOpsRestHandles.LOGIN)

        try:
            headers = {"Content-Type": "application/json;charset=UTF-8"}
            r = requests.post(url, data=json.dumps(payload), headers=headers)
            if r.ok:
                token = r.json()
                self._token = token['token']
                return self._token
            else:
                raise MLOpsException('Call {} with payload {} failed text:[{}]'.format(url, payload, r.text))
        except requests.exceptions.ConnectionError as e:
            self._error(e)
            raise MLOpsException("Connection to MLOps server [{}:{}] refused".format(self._mlops_server, self._mlops_port))
        except Exception as e:
            raise MLOpsException('Call ' + str(url) + ' failed with error ' + str(e))

    def url_get_model_list(self):
        """
        Create the REST request for the model list
        :return: the URL for the REST request for the model list
        """
        return build_url(self._mlops_server, self._mlops_port, self._prefix, MLOpsRestHandles.MODELS)

    def get_model_list(self):
        """
        Requests the list of models from MLOps
        :return: the list of models
        """
        url = self.url_get_model_list()
        return self._get_url_request_response_as_json(url)

    def get_last_approved_model(self, workflow_run_id, pipeline_inst_id):
        url = build_url(self._mlops_server, self._mlops_port, self._prefix, MLOpsRestHandles.MODELS,
                        ionId=workflow_run_id, pipelineInstanceId=pipeline_inst_id, modelType="lastApproved")
        return self._get_url_request_response_as_json(url)

    def post_model_as_file(self, model_file_path, params, metadata):
        """
        Posts a file to the server
        :param model_file_path: model file to upload
        :param params: parameters dictionary
        :param metadata: extended metadata(currently not used with rest connected)
        :return: model_id
        """

        if metadata and not isinstance(metadata, ModelMetadata):
            raise MLOpsException("metadata argument must be a ModelMetadata object, got {}".format(type(metadata)))

        required_params = ["modelName", "modelId", "format", "workflowInstanceId", "pipelineInstanceId", "description"]

        for param_name in required_params:
            if param_name not in params:
                raise MLOpsException('parameter {} is required for publishing model'.format(param_name))

        url = build_url(self._mlops_server, self._mlops_port, MLOpsRestHandles.MODELS, params["pipelineInstanceId"])

        files = {'file': ("file", open(model_file_path, 'rb'), "application/octet-stream")}

        try:
            r = requests.post(url, files=files, params=params, cookies=self._return_cookie())
            if r.ok:
                return r.json()
            else:
                raise MLOpsException('Call {} with filename {} failed text:[{}]'.format(url, model_file_path, r.text))
        except requests.exceptions.ConnectionError as e:
            self._error(e)
            raise MLOpsException("Connection to MLOps server [{}:{}] refused".format(self._mlops_server, self._mlops_port))
        except Exception as e:
            raise MLOpsException('Call ' + str(url) + ' failed with error ' + str(e))

    def url_login(self):
        """
        Create the REST request for the login
        :return: the URL for the REST request
        """
        return build_url(self._mlops_server, self._mlops_port, self._prefix, MLOpsRestHandles.LOGIN)

    def url_get_groups(self):
        """
        Create the REST request for the groups list
        :return: the URL for the REST request
        """
        return build_url(self._mlops_server, self._mlops_port, self._prefix, MLOpsRestHandles.GROUPS)

    def url_get_ees(self):
        """
        Create the REST request for the ee list
        :return: the URL for the REST request
        """
        return build_url(self._mlops_server, self._mlops_port, self._prefix, MLOpsRestHandles.EES)

    def get_groups(self):
        """
        Requests the list of groups from MLOps
        :return: the list of groups
        """
        url = self.url_get_groups()
        return self._get_url_request_response_as_json(url)

    def get_ees(self):
        """
        Requests the list of execution environment from MLOps
        :return: the list of ee
        """
        url = self.url_get_ees()
        return self._get_url_request_response_as_json(url)

    def url_get_agents(self):
        """
        Create the REST request for the agents list
        :return: the URL for the REST request
        """
        return build_url(self._mlops_server, self._mlops_port, self._prefix, MLOpsRestHandles.AGENTS)

    def get_agents(self):
        """
        Requests the list of agents from MLOps
        :return: the list of agents
        """
        url = self.url_get_agents()
        return self._get_url_request_response_as_json(url)

    def url_get_workflow_instance(self, ion_instance_id):
        """
        Create the REST request for the workflow instance for the given ION
        :param ion_instance_id: ION to get the workflow instance of
        :return: the URL for the REST request
        """
        return build_url(self._mlops_server, self._mlops_port, self._prefix, MLOpsRestHandles.WORKFLOW_INSTANCES_V2, ion_instance_id)

    def get_workflow_instance(self, ion_instance_id):
        """
        Requests the workflow instance for the given ION
        :param ion_instance_id: the ION to get the workflow instance of
        :return: the workflow instance
        """
        url = self.url_get_workflow_instance(ion_instance_id)
        return self._get_url_request_response_as_json(url)

    def url_get_health_thresholds(self, ion_instance_id):
        return build_url(self._mlops_server, self._mlops_port, self._prefix, MLOpsRestHandles.HEALTH_THRESHOLDS, ionInstanceId=ion_instance_id, thresholdType='all')

    def get_health_thresholds(self, ion_instance_id):
        url = self.url_get_health_thresholds(ion_instance_id)
        return self._get_url_request_response_as_json(url)

    def download_model(self, model_id):
        """
        Requests the model corresponding to the given model_id
        :param model_id: ID of the model
        :return: the model
        :raises MLOpsException
        """
        url = build_url(self._mlops_server, self._mlops_port, self._prefix, MLOpsRestHandles.MODELS, model_id, MLOpsRestHandles.DOWNLOAD)
        self._info("Downloading model [{}]".format(url))
        r = self._get_url_request_response(url)
        return r.content

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
        url = build_url(self._mlops_server, self._mlops_port, self._prefix, MLOpsRestHandles.STATS, statName=stat_name,
                        workflowNodeId=workflow_node_id, agentId=agent_id, pipelineId=pipeline_id, ionId=ion_id,
                        start=start_time, end=end_time)
        return self._get_url_request_response_as_json(url)

    def post_event(self, pipeline_inst_id, event):
        """
        Post event using protobuf format
        :param pipeline_inst_id:  pipeline instance identifier
        :param event:             ReflexEvent based on protobuf
        :return:                  Json response from agent
        """
        url = build_url(self._mlops_server, self._mlops_port, MLOpsRestHandles.EVENTS, pipeline_inst_id)
        try:
            payload = encoder(event)
            headers = {"Content-Type": "application/json;charset=UTF-8"}
            r = requests.post(url, data=payload, headers=headers, cookies=self._return_cookie())
            if r.ok:
                return r.json()
            else:
                raise MLOpsException('Call {} with payload {} failed text:[{}]'.format(url, event, r.text))
        except requests.exceptions.ConnectionError as e:
            self._error(e)
            raise MLOpsException("Connection to MLOps agent [{}:{}] refused".format(self._mlops_server, self._mlops_port))
        except Exception as e:
            raise MLOpsException('Call ' + str(url) + ' failed with error ' + str(e))

    def post_stat(self, pipeline_inst_id, stat):
        """
        Post stat to agent using json formatting
        :param pipeline_inst_id: pipeline instance identifier
        :param stat:             stat in json format
        :return:                 Json response from agent
        """
        if pipeline_inst_id is None:
            self._error("Missing pipeline instance id cannot post stat")
            return

        url = build_url(self._mlops_server, self._mlops_port, MLOpsRestHandles.STATS,
                        pipeline_inst_id, statType="accumulator")
        try:
            payload = encoder(stat)
            headers = {"Content-Type": "application/json;charset=UTF-8"}
            r = requests.post(url, data=payload, headers=headers, cookies=self._return_cookie())
            if r.ok:
                return r.json()
            else:
                raise MLOpsException('Call {} with payload {} failed text:[{}]'.format(url, stat, r.text))
        except requests.exceptions.ConnectionError as e:
            self._error(e)
            raise MLOpsException("Connection to MLOps agent [{}:{}] refused".format(self._mlops_server, self._mlops_port))
        except Exception as e:
            raise MLOpsException('Call ' + str(url) + ' failed with error ' + str(e))

    def url_get_model_stats(self, model_id):
        return build_url(self._mlops_server, self._mlops_port, self._prefix, MLOpsRestHandles.MODEL_STATS, modelId=model_id)

    def get_model_stats(self, model_id):
        url = self.url_get_model_stats(model_id)
        return self._get_url_request_response_as_json(url)

    def get_alerts(self, query_args):
        url = build_url(self._mlops_server, self._mlops_port, self._prefix, MLOpsRestHandles.EVENTS, **query_args)
        return self._get_url_request_response_as_json(url)

    def url_get_uuid(self, type):
        """
        Create the REST request for getting UUID for an MLObject of type
        :return: the URL for the REST request
        """
        return build_url(self._mlops_server, self._mlops_port, self._prefix, MLOpsRestHandles.UUID, type=type)

    def get_uuid(self, type):
        url = self.url_get_uuid(type)
        return self._get_url_request_response_as_json(url)["id"]
