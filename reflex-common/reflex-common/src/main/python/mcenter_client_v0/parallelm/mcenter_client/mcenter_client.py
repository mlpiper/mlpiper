""" Collection of ECO REST API calls """
import requests
import json
import logging
import urllib
import re
import ntpath
import time
import os

try:  # python3
    from urllib.parse import urlencode
except ImportError:  # python2
    from urllib import urlencode

from parallelm.mcenter_client.http_protocol import HttpProtocol, URL_HIER_PART, URL_SCHEME


class MLOpsRest:
    login = "login"
    auth = "auth"
    agents = "agents"
    ee = "executionEnvironments"
    pipelines = "pipelines"
    pipelinePatternId = "pipelinePatternId"
    profiles = "profiles"
    workflows = "workflows"
    ion = "ion"
    instances = "instances"
    workflowInstances = "workflowInstances"
    models = "models"
    snapshots = "snapshots"
    modelConnector = "modelConnector"
    modelConnectorImport = "import"
    connectorName = "connectorName"


class MCenterAccountType:
    USER = "user"
    SERVICE = "service"


class MCenterAuthType:
    LDAP = "ldap"
    INTERNAL = "internal"


class MCenterClient:

    """
    Control user session
    of eco REST api calls.
    Initializing the class
    does not guarantee the user
    One needs to create the user
    first.

    """
    default_headers = {"Content-Type": "application/json;charset=UTF-8"}

    def __init__(self,
                 user="admin",
                 password="admin",
                 server="localhost",
                 account_type=MCenterAccountType.USER,
                 auth_mode=MCenterAuthType.INTERNAL,
                 protocol=HttpProtocol.http,
                 cert=None,
                 verbose=False):
        """
        :param user:  User name to work with
        :param passwd: User password
        :param server: Server address
        :param account_type: account type
        :param auto_mode:
        :param protocol: Protocol to use HttpProtocol enum should be passed here.

        Cookie is none, until the user is created, and token is obtained
        """

        self.user = user
        self.password = password
        self.cookie = None
        self.server = server
        self._verbose = verbose

        # TODO: this is a very bad programing style, the port should come from the outside.
        self.server_port = '3456'

        if isinstance(protocol, str):
            protocol = HttpProtocol.from_str(protocol)

        self.protocol = protocol
        self.protocol_str = HttpProtocol.protocol_prefix(self.protocol)
        self.cert_path = None

        self.logger = logging.getLogger(self.__class__.__name__)
        self.headers = {"Content-Type": "application/json;charset=UTF-8"}
        self.auth = {}
        self.account_type = account_type
        self.auth_mode = auth_mode

    def set_user(self, user):
        self.user = user
        return self

    def set_password(self, password):
        self.password = password
        return self

    def set_protocol(self, protocol):
        self.protocol = protocol
        return self

    def set_cert(self, cert_path):
        self.cert_path = cert_path
        return self

    def _build_url(self, *res, **params):
        """
        Function to build url. Examples:
        build_url('localhost','3456', 'agents') => http://localhost:3456/agents
        build_url('localhost','3456', 'agents', startTime=12345678) => http://localhost:3456/agents?startTime=12345678
        :param res: REST requests parts
        :param params: REST request parameters
        """
        base_url = self.protocol_str + URL_SCHEME + URL_HIER_PART
        base_url += self.server

        port = str(self.server_port)
        if port:
            base_url += URL_SCHEME + str(port)

        url = base_url
        for r in res:
            if r is not None:
                url = '{}/{}'.format(url, r)
        if params:
            url = '{}?{}'.format(url, urlencode(params))

        if self._verbose:
            print(url)

        return url

    def login(self):
        """
        To be called for getting token.
        Eco rest api call not requiring token
        Example: curl -H "Content-Type: application/json"
        -X POST -d '{"username":"admin","password":"admin"}'
        http://bran-c27:3456/auth/login
        """
        payload = {"username": self.user, "password": self.password, "authMode": self.auth_mode}
        url = self._build_url(MLOpsRest.auth, MLOpsRest.login)
        try:
            r = requests.post(url, data=json.dumps(payload), headers=self.headers, timeout=5, verify=self.cert_path)
            if r.ok:
                token = r.json()
                auth = token['token']
                self.cookie = {'token': auth}
                return self.cookie
            else:
                self.logger.error('Call {} with payload {} failed'.format(url, payload))
                self.logger.error(r.text)
                return None
        except requests.exceptions.ConnectionError as e:
            self.logger.error(e)
            raise Exception("Connection to ECO server {} refused".format(self.server))
        except Exception as e:
            self.logger.error('Call ' + str(url) + ' failed with error ' + str(e))
            return None

    def post(self, url, payload=None, timeout=5, user_headers=default_headers):
        """
        Post request wrapper
        :param url:
        :param payload:
        :param timeout:
        :return:
        """

        if not self.cookie:
            self.login()

        try:
            r = requests.post(url, data=payload, headers=user_headers, cookies=self.cookie, timeout=timeout,
                              verify=self.cert_path)
            if r.ok:
                return r.json()
            else:
                self.logger.error('Call {} with payload {} failed'.format(url, payload))
                self.logger.error(r.text)
        except requests.exceptions.ConnectionError as e:
            self.logger.error(e)
            raise Exception("Connection to ECO server {} refused".format(self.server))
        except Exception as e:
            self.logger.error('Call ' + str(url) + ' failed with error ' + str(e))
            return None

    def put(self, url, payload, timeout=5):
        """
        Put request wrapper
        :param payload:
        :param timeout:
        :return:
        """
        if not self.cookie:
            self.login()

        try:
            r = requests.put(url, data=payload, headers=self.headers, cookies=self.cookie, verify=self.cert_path)
            if r.ok:
                return r.json()
            else:
                self.logger.error(r.text)
        except requests.exceptions.ConnectionError as e:
            self.logger.error(e)
            raise Exception("Connection to ECO server {} refused".format(self.server))
        except Exception as e:
            self.logger.error(e)
            raise e

    def get(self, url, payload={}, timeout=5):
        """
        Get request wrapper
        :param url:
        :param payload:
        :return:
        """
        if not self.cookie:
            self.login()

        try:
            r = requests.get(url,
                             headers=self.headers,
                             timeout=timeout,
                             cookies=self.cookie,
                             verify=self.cert_path)
            if r.ok:
                return r.json()
            else:
                self.logger.error(r.text)
        except requests.exceptions.ConnectionError as e:
            self.logger.error(e)
            raise Exception("Connection to ECO server {} refused".format(self.server))
        except Exception as e:
            e.with_traceback()
            self.logger.error(e)
            return None

    def delete(self, url, paylaod={}):
        """
        Delete request wrapper
        :param url:
        :param paylaod:
        :return:
        """
        if not self.cookie:
            self.login()

        try:
            r = requests.delete(url, headers=self.headers, timeout=5, cookies=self.cookie, data=paylaod,
                                verify=self.cert_path)
            if r.ok:
                return r.json()
            else:
                self.logger.error('Failed to delete with url {}'.format(url))
                self.logger.error(r.text)
        except requests.exceptions.ConnectionError as e:
            self.logger.error(e)
            raise Exception("Connection to ECO server {} refused".format(self.server))
        except Exception as e:
            self.logger.error(e)
            raise e

    def create_agent(self, node):
        """
        Agent can be created
        by admin
        :param server:
        :param node:
        :param payload:
        :return:
        """
        payload = json.dumps({"address": node})
        url = self._build_url(MLOpsRest.agents)
        r = self.post(url, payload)
        return r

    def create_agent_with_payload(self, payload):
        """
        Create passing
        the payload,
        return id or None
        """
        payload = json.dumps(payload)
        url = self._build_url(MLOpsRest.agents)
        r = self.post(url, payload)
        return r

    def update_agent(self, agent_id, payload):
        """ Update agent with new data """
        url = self._build_url(MLOpsRest.agents, agent_id)
        payload = json.dumps(payload)
        r = self.put(url, payload)
        return r

    def get_agent(self, agent_id):
        """ Get agent by id """
        url = self._build_url(MLOpsRest.agents, agent_id)
        r = self.get(url)
        return r

    def list_agents(self):
        """ return agents as json dict """
        url = self._build_url(MLOpsRest.agents)
        r = self.get(url)
        return r

    def delete_agent(self, agent_id):
        """
        Note: currently agents can
        be created and deleted
        by admin only
        :param server:
        :param agent_id:
        :param created_by:
        :return:
        """
        url = self._build_url(MLOpsRest.agents, agent_id)
        r = self.delete(url)
        return r

    def create_ee(self, name, data):
        return ""

    def list_ee(self, name, data):
        return ""

    def get_ee(self, ee_id):
        return ""

    def delete_ee(self, ee_id):
        return ""

    def update_ee(self, ee_id, data):
        return ""

    def create_pipeline_pattern(self, data):
        """
        Create ECO pipeline, returns its ID
        """
        payload = urllib.parse.quote(json.dumps(data), encoding='utf-8')
        self.logger.debug(payload)
        url = self._build_url(MLOpsRest.pipelines)
        r = self.post(url, payload)
        if r:
            return r.get('id')
        return None

    def create_pipeline_profile(self, data):
        """
        Create ECO pipeline, returns its ID
        """
        payload = urllib.parse.quote(json.dumps(data), encoding='utf-8')
        self.logger.debug(payload)
        url = self._build_url(MLOpsRest.profiles, MLOpsRest.pipelines)
        r = self.post(url, payload)
        if r:
            return r.get('id')
        return None

    def list_pipeline_patterns(self):
        url = self._build_url(MLOpsRest.pipelines)
        r = self.get(url)
        return r

    def list_pipeline_profiles(self):
        url = self._build_url(MLOpsRest.profiles, MLOpsRest.pipelines)
        r = self.get(url)
        return r

    def update_pipeline(self, pipeline_id, data):
        """
        Update ECO pipeline, returns the whole pipeline content
        """
        payload = urllib.parse.quote(json.dumps(data), encoding='utf-8')
        self.logger.debug(payload)
        url = self._build_url(MLOpsRest.pipelines, pipeline_id)
        r = self.put(url, payload)
        if r:
            return r
        return None

    def find_pipeline_pattern(self, p_name, user_name):
        """ Get pipeline id by name """
        all_p = self.list_pipelines()
        pipelines = [x for x in all_p if x['name'] == p_name and x['createdBy'] == user_name]
        if pipelines:
            return pipelines[0]
        return None

    def find_pipeline_profile(self, p_name):
        """ Get pipeline id by name """
        all_p = self.list_pipeline_profiles()
        pipes = [x for x in all_p if x['name'] == p_name]
        return pipes

    def get_pipeline_pattern(self, p_id):
        """
        Return pipeline info
        query by pipeline id
        """
        url = self._build_url(MLOpsRest.pipelines, pipelinePatternId=p_id)
        r = self.get(url)
        if isinstance(r, list):
            pattern = [x for x in r if x['id'] == p_id]
            if pattern:
                return pattern[0]
        return None

    def get_pipeline_profile(self, p_id):
        """
        Return pipeline profile info
        query by pipeline profile id
        """
        url = self._build_url(MLOpsRest.profiles, MLOpsRest.pipelines, pipelineProfileId=p_id)
        r = self.get(url)
        return r

    def delete_pipeline_pattern(self, p_id):
        """ Remove workflow from database """
        url = self._build_url(MLOpsRest.pipelines, p_id)
        r = self.delete(url)
        return r

    def delete_pipeline_profile(self, p_id):
        """ Remove pipeline profile from database """
        url = self._build_url(MLOpsRest.profiles, MLOpsRest.pipelines, pipelineProfileId=p_id)
        r = self.delete(url)
        return r

    def create_ion_pattern(self, data):
        """ Create workflow in database, return its ID or None """
        payload = json.dumps(data)
        url = self._build_url(MLOpsRest.workflows)
        r = self.post(url, payload)
        if r:
            return r.get('id')

    def create_ion_profile(self, data):
        """ Create ion profile in database, return its ID or None """
        payload = json.dumps(data)
        url = self._build_url(MLOpsRest.profiles, MLOpsRest.workflows, "ion")
        r = self.post(url, payload)
        if r:
            return r.get('id')

    def update_workflow(self, workflow_id, data):
        """
        Update the a workflow with the given data
        :param workflow_id:
        :param data:
        :return: workflow details
        """
        payload = json.dumps(data)
        url = self._build_url(MLOpsRest.workflows, workflow_id)
        r = self.put(url, payload)
        if r:
            return r

    def launch_workflow(self, wid, user, annotation, sandbox="true", run_now="true", run_iterations=-1):
        """ Launches workflow, return workfloRunId or None """
        sandbox = str(sandbox).lower()  # ensure that json.loads handle this field correctly
        data = {"sandbox": sandbox, "runNow": run_now, "runIterations": run_iterations, "serviceAccount": user}
        if sandbox == 'false':
            annotation = urllib.parse.quote(annotation,  encoding='utf-8')
            data["annotation"] = annotation
        payload = json.dumps(data)
        url = self._build_url(MLOpsRest.workflows, wid,  "run")
        r = self.post(url, payload)
        return r.get('workflowRunId')

    def launch_workflow_sandbox(self, wid, run_now="true", run_iterations=-1):
        """ Launches workflow in sandbox mode, return workfloRunId or None """
        data = {"sandbox": "true", "runNow": run_now, "runIterations": run_iterations}
        payload = json.dumps(data)
        url = self._build_url(MLOpsRest.workflows, wid, 'run')
        r = self.post(url, payload)
        return r

    def get_workflow(self, workflow_id):
        """
        Get the workflow details for the given workflow id
        :param workflow_id:
        :return: workflow details
        """
        url = self._build_url(MLOpsRest.workflows, workflow_id)
        r = self.get(url)
        return r

    def get_workflow_instance(self, workflow_run_id):
        """
        Get the workflow instance details for the given workflow run id
        :param workflow_run_id:
        :return: workflow instance details
        """
        url = self._build_url(MLOpsRest.workflows, "instances", workflow_run_id)
        r = self.get(url)
        return r

    def list_ion_patterns(self):
        """ Return json dict of all existing workflows """
        url = self._build_url(MLOpsRest.workflows)
        r = self.get(url)
        return r

    def list_ion_profiles(self):
        """ Return json dict of all existing ion profiles """
        url = self._build_url(MLOpsRest.profiles, MLOpsRest.workflows)
        r = self.get(url)
        return r

    def list_workflow_instances(self):
        """ Return json dict of workflow instances"""
        url = self._build_url(MLOpsRest.workflowInstances)
        r = self.get(url)
        return r

    def delete_workflow_instance(self, p_id):
        """ Remove workflow from database """
        url = self._build_url(MLOpsRest.workflows, MLOpsRest.instances, p_id, 'remove')
        r = self.delete(url)
        return r

    def stop_workflow(self, jobId):
        """ Stop running workflow """
        url = self._build_url(MLOpsRest.workflows, MLOpsRest.instances, jobId, 'terminate')
        r = self.delete(url)
        return r

    def remove_workflow_instance(self, workflow_run_id):
        """
        Remove the given worklfow instance
        :param workflow_run_id:
        :return:
        """
        url = self._build_url(MLOpsRest.workflows, MLOpsRest.instances, workflow_run_id, 'remove')
        r = self.delete(url)
        return r

    def find_ion_pattern(self, w_name, user_name):
        """
        Get workflow ids by name.
        Currently it is possible
        to create multiple workflows
        with same name.
        :returns: list
        """
        all_w = self.list_ion_patterns()
        workflows = [x['id'] for x in all_w if x['name'] == w_name and x['createdBy'] == user_name]
        if workflows:
            return workflows[0]
        return None

    def find_ion_profile(self, w_name):
        """
        Get workflow ids by name.
        Currently it is possible
        to create multiple workflows
        with same name.
        :returns: list
        """
        all_w = self.list_ion_profiles()
        profiles = [x['id'] for x in all_w if x['name'] == w_name]
        return profiles

    def delete_ion_pattern(self, w_id):
        url = self._build_url(MLOpsRest.workflows, w_id)
        r = self.delete(url)
        return r

    def delete_ion_profile(self, w_id):
        url = self._build_url(MLOpsRest.profiles, MLOpsRest.ion, ionProfileId=w_id)
        r = self.delete(url)
        return r

    def workflow_details(self, w_id):
        url = self._build_url(MLOpsRest.workflows, w_id)
        r = self.get(url)
        return r

    def ion_pattern_details(self, w_id):
        url = self._build_url(MLOpsRest.workflows, w_id)
        r = self.get(url)
        return r[0]

    def running_workflow_details(self, w_rid):
        url = self._build_url(MLOpsRest.workflows, MLOpsRest.instances, w_rid)
        r = self.get(url)
        return r

    def move_to_production(self, w_id, service_account_username, annotation):
        url = self._build_url(MLOpsRest.workflows, MLOpsRest.instances, w_id,
                              'moveToProduction', serviceAccount=service_account_username, annotation=annotation)
        r = self.post(url)
        return r

    # MODELS
    def get_models(self, timeout=25):
        """
        Query models.
        Larger timeout
        is for larger models
        """
        url = self._build_url(MLOpsRest.models)
        r = self.get(url, timeout)
        return r

    def get_model(self, modelId, timeout=25):
        """
        Query models.
        Larger timeout
        is for larger models
        """
        url = self._build_url(MLOpsRest.models, modelId=modelId)
        r = self.get(url)
        return r

    def get_model_by_ion_id(self, ion_id, timeout=25):
        """
        Query models by ION id.
        Larger timeout
        is for larger models
        """
        url = 'http://' + self.server + ':' + self.server_port + '/models?ionId=' + str(ion_id)
        r = self.get(url)
        return r

    def delete_model(self, m_id):
        """ Remove model by id """
        url = self._build_url(MLOpsRest.models, m_id)
        r = self.delete(url)
        return r

    def download_model(self, m_id, file_pth=None):
        """
        Download model by id.
        """
        url = self._build_url(MLOpsRest.models, m_id, 'download')
        r = requests.get(url, cookies=self.cookie, timeout=5)
        if r.reason == 'OK' and file_pth is not None:
            with open(file_pth, mode='w') as f:
                f.write(r.content.decode("utf-8"))
        return r

    def post_model(self, model_path, model_name, model_format):
        """
        Not implemented yet
        :param server:
        :param model_path:
        :param model_name:
        :return:
        """
        if not self.cookie:
            self.login()
        url = self._build_url(MLOpsRest.models)
        params = (
            ('modelName', model_name),
            ('token', self.user),
            ('format', model_format)
        )

        files = {
            'fi': ('filePath', open(model_path, 'rb'), "multipart/form-data")
        }

        r = requests.post(url, params=params, files=files, cookies=self.cookie, timeout=15)
        self.logger.info(r.reason)
        if r.ok:
            return r.json()
        return None

    def upload_model(self, model_name, model_path, model_format, description=''):
        """
        Uploads components to ECO server
        """
        return self.post_model(model_name=model_name, model_path=model_path, model_format=model_format)

    def model_status(self, model_id):
        """
        Get the status of the given model
        :param model_id:
        :return:
        """
        url = self._build_url(MLOpsRest.models, 'status')
        return self.model_get_request_with_id(url, model_id)

    def model_usage(self, model_id):
        """
        Get the usage of the given model
        :param model_id:
        :return:
        """
        url = self._build_url('modelsUsage')
        return self.model_get_request_with_id(url, model_id)

    def model_provenance(self, model_id):
        """
        Get the model provenance of the given model
        :param model_id:
        :return:
        """
        url = self._build_url(MLOpsRest.models, 'source')
        return self.model_get_request_with_id(url, model_id)

    def model_get_request_with_id(self, url, model_id):
        """
        This function will construct the params with the model id and call the get request
        :param url:
        :param model_id:
        :return:
        """
        params = (
            ('modelId', model_id),
            ('token', self.user)
        )

        try:
            r = requests.get(url, params=params, cookies=self.cookie, timeout=5)
            if r.ok:
                return r.json()
            else:
                self.logger.error(r.text)
                return None
        except Exception as e:
            e.with_traceback()
            self.logger.error("Call " + str(url) + " failed with error", e)
            return None

    def set_model(self, model_id, ion_id, pipeline_instance_id):

        url = self._build_url(MLOpsRest.workflows,
                              MLOpsRest.instances,
                              ion_id,
                              'modelUpdate',
                              modelId=model_id,
                              pipelineInstanceId=pipeline_instance_id)
        r = self.post(url)
        return r

    def model_connector_list(self):
        url = self._build_url(MLOpsRest.modelConnector)
        r = self.get(url)
        return r

    def model_connector_import(self, model_name, model_description, model_connector_name, model_connector_args):
        url = self._build_url(MLOpsRest.modelConnector,
                              MLOpsRest.modelConnectorImport,
                              connectorName=model_connector_name,
                              modelName=model_name,
                              description=model_description)
        r = self.post(url, payload=json.dumps(model_connector_args))
        return r


    # SNAPSHOTS
    def create_snapshot(self, w_id, name):
        """
        Create ECO snapshot, returns its ID
        """
        data = {"workflowInstanceId": w_id, "name": name}
        payload = json.dumps(data)
        url = self._build_url(MLOpsRest.snapshots)
        r = self.post(url, payload)
        if r:
            return r.get('id')
        return None

    def list_snapshots(self):
        url = self._build_url(MLOpsRest.snapshots)
        r = self.get(url)
        return r

    def find_snapshot(self, name, user_name):
        """
        Get snapshot id
        given the name.
        :returns: string
        Returns snapshot id
        or None if not found
        """
        all_w = self.list_snapshots()
        snapshots = [x['id'] for x in all_w if x['name'] == name and x['createdBy'] == user_name]
        if snapshots:
            return snapshots[0]
        return None

    def snapshot_details(self, s_id):
        """
        Snapshot details given
        the snapshot id
        """
        url = self._build_url('snapshots', snapshotId=s_id)
        r = self.get(url)
        return r

    def delete_snapshot(self, p_id):
        """ Remove snapshot by snapshot id """
        url = self._build_url('snapshots', p_id)
        r = self.delete(url)
        return r

    def export_snapshot(self, p_id, outpath=None, timeout=5):
        """
        Export snapshot
        If outpath is not None,
        will be saved to the path,
        of format specified as file
        extension.
        Otherwise will return
        the archive as binary
        """
        url = self._build_url('snapshots', p_id, 'export')
        r = requests.get(url, cookies=self.cookie, timeout=timeout)
        if r.reason == 'OK' and outpath is not None:
            with open(outpath, mode='bw') as f:
                f.write(r.content)
        return r

    def prepare_snapshot(self, snapshot_id):
        """
        Prepare snapshot for export
        :param server:
        :param snapshot_id:
        :return:
        """
        url = self._build_url('snapshots', snapshot_id, 'prepare')
        r = self.post(url)
        return r

    def check_prepare_snapshot_status(self, snapshot_id):
        """
        Check the status of preparation.
        Returns text whatever it is,
        the caller will process the text
        per needs, even if it is Failed

        :param server:
        :param snapshot_id:
        :return: String, choice of Complete, In Progress, Failed
        """
        url = self._build_url('snapshots', snapshot_id, 'prepareStatus')
        # Because this request returns text,
        # while EcoActions.get() return json, so call directly
        r = requests.get(url, cookies=self.cookie)
        if r.reason == 'OK':
            return r.text
        else:
            return None

    # EVENTS
    def get_events(self, event_id=None):
        if event_id:
            url = self._build_url('events', event_id)
        else:
            url = self._build_url('events')
        r = self.get(url, timeout=50)
        return r

    # USER MANAGEMENT
    def get_users(self):
        url = self._build_url('users')
        r = self.get(url)
        return r

    def get_user_me(self):
        url = self._build_url('users', 'me')
        r = self.get(url)
        return r

    def create_user(self, dataload):
        """

        :param self:
        :param server:
        :param dataload:
        :return:
        """
        dataload = json.dumps(dataload)
        url = self._build_url('users')
        r = self.post(url, dataload)
        return r

    def delete_user(self, dataload):
        dataload = json.dumps(dataload)
        url = self._build_url('users')
        r = self.delete(url, dataload)
        return r

    def update_user(self, dataload):
        dataload = json.dumps(dataload)
        url = self._build_url('users')
        r = self.put(url, dataload)
        return r

    def create_role(self,
                    roleName,
                    roleDesc=None,
                    ionPerms=0,
                    prodIonInstancePerms=0,
                    sandboxIonInstancePerms=0,
                    modelPerms=0,
                    pipelinePerms=0,
                    pipelineComponentPerms=0,
                    profilePerms=0):
        """
        create roles
        default role is : 0
        curl -X POST 'http://localhost:3456/roles?roleName=Data%20Scientist&roleDesc=A%20Data%20Scientist%20Person&ionPerms=6&ionInstancePerms=4&pipelinePerms=7&pipelineComponentPerms=5&modelPerms=2'
        """
        if roleDesc is not None:
            roleDesc = urllib.parse.quote(roleDesc, encoding='utf-8')

        roleName =  urllib.parse.quote(roleName, encoding='utf-8')
        url = 'http://' + self.server + ':' + self.server_port + \
              '/roles?roleName=' + str(roleName) + \
              '&roleDesc=' + str(roleDesc) + \
              '&ionPerms=' + str(ionPerms) + \
              '&prodIonInstancePerms=' + str(prodIonInstancePerms) + \
              '&sandboxIonInstancePerms=' + str(sandboxIonInstancePerms) + \
              '&pipelinePerms=' + str(pipelinePerms) + \
              '&pipelineComponentPerms=' + str(pipelineComponentPerms) + \
              '&modelPerms=' + str(modelPerms) + \
              '&profilePerms=' + str(profilePerms)

        r = self.post(url)
        if r:
            return r.get('id')
        return None

    def get_roles(self, ids=None):
        """
        Returns roles in json of specidfied IDs
        """
        if not ids:
            url = 'http://' + self.server + ':' + self.server_port + '/roles'
        else:
            append_url = ''
            base_url = 'http://' + self.server + ':' + self.server_port + '/roles?'
            for i in range(len(ids)):
                append_url += '&roleId=' + str(ids[i])

            url = base_url + append_url

        r = self.get(url)
        return r

    def delete_role(self, role_id):
        """
        Delete role
        returns empty {}
        """
        url = 'http://' + self.server + ':' + self.server_port + '/roles/' + str(role_id)
        r = self.delete(url)
        return r

    def update_role(self, role_id, role_info):
        """
        Delete role
        returns updated roles json
        """
        base_url = 'http://' + self.server + ':' + self.server_port + '/roles/' + str(role_id) + '?'
        append_url = ""

        for key, value in role_info.items():
            append_url += "&" + str(key) + "=" + str(value)
        url = base_url + append_url

        r = self.put(url, None)
        if r:
            return r
        return None

    def find_roleid(self, role_name):
        """
        Given a role_name,
        returns roleId
        or None if not found
        """
        all_roles = self.get_roles()
        for each_role in all_roles:
            if each_role['name'] == role_name:
                return each_role["id"]
        return None

    def validate_user(self, headers):
        url = self._build_url('auth', 'validate')
        r = self.post(url, None, 5, headers)
        return r

    # ALERTS
    def get_alerts(self):
        url = self._build_url('alerts')
        r = self.get(url)
        return r

    # VIEWS
    def get_l1_view(self, start=None, end=None):
        """
        Returns Level 1 view details
        :param start:
        :param: end:
        :return: json
        """
        d = {}
        if start:
            d['start_time'] = start
        if end:
            d['end_time'] = end

        url = self._build_url('views', 'l1', **d)
        r = self.get(url)
        return r

    def get_l2_view(self,
                    object_id,
                    pipeline_instance_id_A,
                    pipeline_instance_id_B=None,
                    get_by='ionId',
                    start=None,
                    end=None):
        """
        Returns Level2 view details
        per user defined ECO object ID,
        running ION,
        event, or snapshot
        :param get_by:
        :param pipeline_instance_id_A:
        :param pipeline_instance_id_B:
        :param start:
        :param: end:
        :return: json
        """
        d = {}

        if not object_id:
            self.logger.error('L2 View queried with invalid ID: ' + str(object_id))
            return None
        d['pipelineInstanceA'] = pipeline_instance_id_A

        if pipeline_instance_id_B is not None:
            d['pipelineInstanceB'] = pipeline_instance_id_B

        if get_by.count('snapshot'):
            d['snapshotId'] = object_id
        elif get_by.count('event'):
            d['eventId'] = object_id
        else:
            d['ionId'] = object_id

        if start:
            d['start'] = start
        if end:
            d['end'] = end

        url = self._build_url('views', 'l2', **d)
        r = self.get(url)
        return r

    def get_l2_canary_view(self,
                           ion_id,
                           pipeline_instance_id_A,
                           pipeline_instance_id_B,
                           start=None,
                           end=None):
        """
        :param ion_id:
        :param pipeline_instance_id_A:
        :param pipeline_instance_id_B:
        :param start:
        :param: end:
        :return: json
        Returns Canary Level2 view details for running ION, instance id
        If event_id is not None, query will be by event
        """
        d = {}
        if start:
            d['start'] = start
        if end:
            d['end'] = end
        d['ionId'] = ion_id
        d['pipelineInstanceA'] = pipeline_instance_id_A
        d['pipelineInstanceB'] = pipeline_instance_id_B
        d['type'] = 'canary'
        url = self._build_url('views', 'l2', **d)
        r = self.get(url)
        return r

    def get_l3_view(self, object_id, get_by='ionId', start=None, end=None):
        """
        :param object_id:
        :param get_by
        :param start:
        :param: end:
        :return: json
        Returns Level3 view details
        per user defined ECO object ID,
        running ION,
        event, or snapshot
        """
        if not object_id:
            self.logger.error('L3 View queried with invalid ID: {}'.format(object_id))
            return None
        d = {}
        if start:
            d['start'] = start
        if end:
            d['end'] = end
        if get_by.count('snapshot'):
            d['snapshotId'] = object_id
        elif get_by.count('event'):
            d['eventId'] = object_id
        url = self._build_url('views', 'l3', **d)
        r = self.get(url)
        return r

    def get_l3_v2_view(self, w_id, wf_node_id, agent_id, pipeline_id):
        """
        Example: http://localhost:7092/api/views/l3/v2?workflowNodeId=0&agentId=d68cd143-0d63-47ea-9346-597a1977ba55&pipelineId=6eb951ed-d7ff-4bbe-8034-94aeba0b511e&workflowInstanceId=2d61daba-f783-4293-a62b-c105a19acd4d&start=1518021475243&end=1518043075243
        Returns L3 v2 view details
        """
        url = self._build_url('views', 'l3', 'v2',
                              workflowNodeId=wf_node_id,
                              agentId=agent_id,
                              pipelineId=pipeline_id,
                              workflowInstanceId=w_id)
        r = self.get(url)
        return r

    # COMPONENTS
    def upload_component(self, file_path, do_store=True, overwrite=True, file_type='tar'):
        """
        Uploads components to ECO server
        """
        if not self.cookie:
            self.login()
        url = self._build_url('components')
        params = (
            ('fileType', file_type),
            ('token', self.user)
        )

        files = {'fi': (ntpath.basename(file_path), open(file_path, 'rb'), "multipart/form-data")}

        r = requests.post(url, params=params, files=files, cookies=self.cookie, timeout=50)
        self.logger.info(r.reason)
        if r.ok:
            json_result = r.json()
            if not do_store:
                return json_result

            return self._store_component(json_result, False, overwrite)

        else:
            raise Exception("Failed uploading component: {}".format(r.text))

    def _store_component(self, upload_info, ask_overwrite, overwrite=False):
        url = 'http://' + self.server + ':' + self.server_port + '/components'
        if upload_info['exists']:
            if ask_overwrite:
                answer = input("The given component already exists, would you like to overwrite it? [yes|no] ")
                upload_info['overwrite'] = True if answer == "yes" else False
            else:
                upload_info['overwrite'] = overwrite

        r = self.put(url, json.dumps(upload_info))
        return r

    def get_components(self):
        """
        Uploads components to ECO server
        """
        url = self._build_url('components')
        r = self.get(url)
        return r

    # HELPERS
    def get_workflow_agent_ids(self, w_id):
        """ Agent ids workflow refers to """
        ids = []
        groups = self.get_workflow_group_ids(w_id)
        for gid in groups:
            details = self.get_group(gid)
            ids = ids + details['agents']
        return ids

    def get_workflow_group_ids(self, w_id):
        """ Group ids workflow refers to """
        ids = []
        details = self.workflow_details(w_id)
        if details:
            details = details.get('workflow')
            ids = [x['groupId'] for x in details]
        return ids

    def get_workflow_pipeline_ids(self, w_id):
        """ Pipeline ids workflow referes to """
        ids = []
        details = self.workflow_details(w_id)
        if details:
            details = details.get('workflow')
            ids = [x['pipelineId'] for x in details]
        return ids

    def get_pipeline_type(self, p_id):
        details = self.get_pipeline(p_id)
        if details:
            return details['engineType']
        return None

    def server_from_url(self, url):
        """
        This is a temporary solution
        Eco Rest api is passing
        standard string for url
        so parsing it to get
        self.server name will work
        for the time being
        """
        result = re.search('http://(.*):3456', url)
        return result.group(1)

    def get_username(self):
        return self.user

    def get_account_type(self):
        return self.account_type

    def prepare_logs(self, workflowInstanceId):
        """
        Prepare logs to download
        """
        url = self._build_url('prepareLogs', ionInstanceId=workflowInstanceId)
        r = self.post(url)
        status = self.check_prepare_logs_status(r['logPrepareId'], workflowInstanceId)
        prepare_timeout = time.time() + 10
        while status['status'] == "In progress":
            if time.time() < prepare_timeout:
                time.sleep(2)
                status = self.check_prepare_logs_status(r['logPrepareId'], workflowInstanceId)
            else:
                continue

        if status['status'] == "Complete":
            return r['logPrepareId']
        else:
            return None

    def check_prepare_logs_status(self, logPrepareId, workflowInstanceId):
        """
        Check status of logs prepared
        """
        url = self._build_url('logs', ionInstanceId=workflowInstanceId, logPrepareId=logPrepareId, action='status')
        r = self.get(url)
        return r

    def download_logs(self, preparedlogId, workflowInstanceId, tar_file=None):
        """
        Download prepared logs
        """
        url = self._build_url('logs', ionInstanceId=workflowInstanceId, logPrepareId=preparedlogId, action='download')
        try:
            r = requests.get(url, cookies=self.cookie, timeout=120, allow_redirects=True)
            if r.reason == 'OK' and tar_file is not None:
                try:
                    f = open(tar_file, "wb")
                except IOError as e:
                    self.logger.error('error: {}'.format(e))
                else:
                    with f:
                        f.write(r.content)
            else:
                self.logger.error('error: {}'.format(r.text))
                return None
        except requests.exceptions.ConnectionError as e:
            self.logger.error(e)
            raise Exception("Connection to ECO server {} refused".format(self.server))
        except Exception as e:
            e.with_traceback()
            self.logger.error(e)
            return None

    # PROFILES/PATTERNS
    def create_pipeline_pattern(self, data):
        """
        Create ECO pipeline, returns its ID
        """
        payload = urllib.parse.quote(json.dumps(data), encoding='utf-8')
        self.logger.debug(payload)
        url = 'http://' + self.server + ':' + self.server_port + '/pipelines'
        r = self.post(url, payload)
        if r:
            return r.get('id')
        return None

    def create_pipeline_profile(self, data):
        """
        Create ECO pipeline, returns its ID
        """
        payload = urllib.parse.quote(json.dumps(data), encoding='utf-8')
        self.logger.debug(payload)
        url = 'http://' + self.server + ':' + self.server_port + '/profiles/pipelines'
        r = self.post(url, payload)
        if r:
            return r.get('id')
        return None

    def list_pipeline_patterns(self):
        url = 'http://' + self.server + ':' + self.server_port + '/pipelines'
        r = self.get(url)
        return r

    def list_pipeline_profiles(self):
        url = 'http://' + self.server + ':' + self.server_port + '/profiles/pipelines'
        r = self.get(url)
        return r

    def find_pipeline_pattern(self, p_name):
        """ Get pipeline id by name """
        all_p = self.list_pipeline_patterns()
        pipes = [x for x in all_p if x['name'] == p_name]
        return pipes

    def find_pipeline_profile(self, p_name):
        """ Get pipeline id by name """
        all_p = self.list_pipeline_profiles()
        pipes = [x for x in all_p if x['name'] == p_name]
        return pipes

    def get_pipeline_pattern(self, p_id):
        """
        Return pipeline pattern info
        query by pipeline pattern id
        """
        url = 'http://' + self.server + ':' + self.server_port +'/pipelines?pipelinePatternId=' + str(p_id)
        r = self.get(url)
        if isinstance(r, list):
            pattern = [x for x in r if x['id'] == p_id]
            if pattern:
                return pattern[0]
        return None

    def get_pipeline_profile(self, p_id):
        """
        Return pipeline profile info
        query by pipeline profile id
        """
        url = 'http://' + self.server + ':' + self.server_port + '/profiles/pipelines?pipelineProfileId=' + str(p_id)
        r = self.get(url)
        if isinstance(r, list):
            return r[0]
        return None

    def delete_pipeline_pattern(self, p_id):
        """ Remove pipeline pattern from database """
        url = 'http://' + self.server + ':' + self.server_port + '/pipelines/' + str(p_id)
        r = self.delete(url)
        return r

    def delete_pipeline_profile(self, p_id):
        """ Remove pipeline profile from database """
        url = 'http://' + self.server + ':' + self.server_port + '/profiles/pipelines?pipelineProfileId=' + str(p_id)
        r = self.delete(url)
        return r

    def update_pipeline_profile(self, profile_id, data):
        """
        Update ECO pipeline, returns the whole pipeline content
        """
        payload = urllib.parse.quote(json.dumps(data), encoding='utf-8')
        self.logger.debug(payload)
        url = 'http://' + self.server + ':' + self.server_port + \
              '/profiles/pipelines/' + str(profile_id)
        r = self.put(url, payload)
        if r:
            return r
        return None

    def create_ion_pattern(self, data):
        """ Create ion pattern in database, return its ID or None """
        payload = json.dumps(data)
        url = 'http://' + self.server + ':' + self.server_port + '/workflows'
        r = self.post(url, payload)
        if r:
            return r.get('id')

    def create_ion_profile(self, data):
        """ Create ion profile in database, return its ID or None """
        payload = json.dumps(data)
        url = 'http://' + self.server + ':' + self.server_port + '/profiles/ion'
        r = self.post(url, payload)
        if r:
            return r.get('id')

    def list_ion_patterns(self):
        """ Return json dict of all existing ion patterns """
        url = 'http://' + self.server + ':' + self.server_port + '/workflows'
        r = self.get(url)
        return r

    def list_ion_profiles(self):
        """ Return json dict of all existing ion profiles """
        url = 'http://' + self.server + ':' + self.server_port + '/profiles/ion'
        r = self.get(url)
        return r

    def find_ion_pattern(self, w_name):
        """
        Get workflow ids by name.
        Currently it is possible
        to create multiple workflows
        with same name.
        :returns: list
        """
        all_w = self.list_ion_patterns()
        workflows = [x['id'] for x in all_w if x['name'] == w_name]
        return workflows

    def find_ion_profile(self, w_name):
        """
        Get workflow ids by name.
        Currently it is possible
        to create multiple workflows
        with same name.
        :returns: list
        """
        all_w = self.list_ion_profiles()
        profiles = [x['id'] for x in all_w if x['name'] == w_name]
        return profiles

    def delete_ion_pattern(self, w_id):
        url = 'http://' + self.server + ':' + self.server_port + '/workflows/' + str(w_id)
        r = self.delete(url)
        return r

    def delete_ion_profile(self, w_id):
        url = 'http://' + self.server + ':' + self.server_port + '/profiles/ion?ionProfileId=' + str(w_id)
        r = self.delete(url)
        return r

    def ion_pattern_details(self, w_id):
        url = 'http://' + self.server + ':' + self.server_port + '/workflows/' + str(w_id)
        r = self.get(url)
        return r[0]

    def ion_profile_details(self, w_id):
        url = 'http://' + self.server + ':' + self.server_port + '/profiles/ion?ionProfileId=' + str(w_id)
        r = self.get(url)
        print(r)
        return r[0]

    def update_ion_profile(self, profile_id, data):
        """
        Update ECO pipeline, returns the whole pipeline content
        """
        payload = urllib.parse.quote(json.dumps(data), encoding='utf-8')
        self.logger.debug(payload)
        url = 'http://' + self.server + ':' + self.server_port + \
              '/profiles/ion/' + str(profile_id)
        r = self.put(url, payload)
        if r:
            return r
        return None

    def get_ssh_public_key(self, key_name):
        url = self._build_url("sourcecontrol", "credentials", "ssh-key", key_name)
        r = self.get(url)
        return r

    def list_ssh_public_keys(self, token='admin'):
        url = self._build_url("sourcecontrol", "credentials", "ssh-key")
        r = self.get(url)
        return r

    def get_ssh_key_post_args(self, key_name, keytype, passphrase):
        d = {"name": key_name}
        if keytype:
            d["type"] = keytype
        if passphrase:
            d["passphrase"] = passphrase
        return d

    def create_ssh_key(self, key_name, keytype=None, passphrase=None):
        d = self.get_ssh_key_post_args(key_name, keytype, passphrase)
        url = self._build_url("sourcecontrol", "credentials", "ssh-key", **d)
        r = self.post(url)
        return r

    def create_ssh_key_from_path(self, private_ssh_key_path, keytype=None, passphrase=None):
        pub_sshkey_path = private_ssh_key_path + ".pub"

        assert os.path.isfile(private_ssh_key_path)
        assert os.path.isfile(pub_sshkey_path)

        key_name = os.path.basename(private_ssh_key_path)

        d = self.get_ssh_key_post_args(key_name, keytype, passphrase)
        url = self._build_url("sourcecontrol", "credentials", "ssh-key", **d)

        files = {
            'pub-key-path': (os.path.basename(pub_sshkey_path), open(pub_sshkey_path, 'rb'),
                             'application/octet-stream'),
            'priv-key-path': (os.path.basename(private_ssh_key_path), open(private_ssh_key_path, 'rb'),
                              'application/octet-stream')
        }

        print("url [{}]".format(url))
        r = requests.post(url, files=files, cookies=self.cookie)
        print(str(r))
        if r.ok:
            return r.json()
        else:
            self.logger.error(r.text)

        return None

    def delete_ssh_key(self, key_name):
        """ delete agent """
        url = self._build_url('sourcecontrol', 'credentials', 'ssh-key', key_name)
        r = self.delete(url)
        return r

    def get_source_control_repo(self, repo_id):
        url = self._build_url('sourcecontrol', 'repos', repo_id, repoId=repo_id)
        print(url)
        r = self.get(url)
        return r

    def list_source_control_repos(self):
        """ return agents as json dict """
        url = self._build_url('sourcecontrol', 'repos')
        r = self.get(url)
        return r

    @staticmethod
    def construct_repo_payload(repo_uri, repo_name, description, credential_type, credential_id):
        return json.dumps({"name": repo_name,
                           "uri": repo_uri,
                           "description": description,
                           "repoType": "GIT",
                           "credentialType": credential_type.upper(),
                           "credentialId": credential_id})

    def create_source_control_repo(self, repo_uri, repo_name=None, description=None, credential_type='NONE',
                                   credential_id=None):
        payload = MCenterClient.construct_repo_payload(repo_uri, repo_name, description, credential_type, credential_id)
        url = self._build_url('sourcecontrol', 'repos')
        r = self.post(url, payload)
        return r

    def update_source_control_repo(self, repo_id, repo_uri=None, repo_name=None, description=None,
                                   credential_type='NONE', credential_id=None):
        payload = MCenterClient.construct_repo_payload(repo_uri, repo_name, description, credential_type,
                                                    credential_id)
        url = self._build_url('sourcecontrol', 'repos', repo_id)
        r = self.put(url, payload)
        return r

    def delete_source_control_repo(self, repo_id):
        url = self._build_url('sourcecontrol', 'repos', repo_id)
        r = self.delete(url)
        return r

    def read_source_control_repo_refs(self, repo_id):
        url = self._build_url('sourcecontrol', 'repos', repo_id)
        r = self.get(url)
        return r

    def list_source_control_repo_components(self):
        url = self._build_url('sourcecontrol', 'components')
        r = self.get(url)
        return r

    def get_source_control_repo_component(self, comp_id):
        url = self._build_url('sourcecontrol', 'components', comp_id)
        r = self.get(url)
        return r

    @staticmethod
    def construct_repo_comp_payload(repo_id, ref_name, rltv_path, metadata_filename, auto_update):
        payload = {"repoId": repo_id,
                   "refName": ref_name,
                   "rltvPath": rltv_path,
                   "autoUpdate": auto_update}

        if metadata_filename:
            payload["metadataFilename"] = metadata_filename

        return json.dumps(payload)

    def create_source_control_repo_component(self, repo_id, ref_name, rltv_path,
                                             metadata_filename=None, created_by='admin'):
        url = self._build_url('sourcecontrol', 'components')
        payload = MCenterClient.construct_repo_comp_payload(repo_id, ref_name, rltv_path, metadata_filename, True)
        r = self.post(url, payload, timeout=60)
        return r

    def update_source_control_repo_component(self, comp_id, repo_id, ref_name, rltv_path,
                                             metadata_filename=None, created_by='admin'):
        url = self._build_url('sourcecontrol', 'components', comp_id)
        payload = MCenterClient.construct_repo_comp_payload(repo_id, ref_name, rltv_path, metadata_filename, True)
        r = self.put(url, payload, timeout=60)
        return r

    def delete_source_control_comp(self, comp_id):
        url = self._build_url('sourcecontrol', 'components', comp_id)
        r = self.delete(url, timeout=60)
        return r

    @staticmethod
    def _get_filename_from_cd(cd):
        """
        Get filename from content-disposition
        """
        if not cd:
            return None
        fname = re.findall('filename=(.+)', cd)
        if len(fname) == 0:
            return None
        return fname[0]

    def get_mcenter_client_package(self, download_dir="/tmp"):

        url = self._build_url("tools", "mcenter_client", "download")
        r = requests.get(url, cookies=self.cookie)
        cd = r.headers.get('content-disposition')

        if cd:
            filename = self._get_filename_from_cd(cd)
        elif url.find('/'):
            filename = url.rsplit('/', 1)[1]
        else:
            filename = "mcenter-client.tgz"

        print("file_name: {}".format(filename))
        filename = os.path.basename(filename)
        pkg_path = os.path.join(download_dir, filename)
        print("file_name: {}".format(pkg_path))
        open(pkg_path, 'wb').write(r.content)
        return r
