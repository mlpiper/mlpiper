import connexion
import six

from mcenter_server_api.models.dashboard import Dashboard  # noqa: E501
from mcenter_server_api.models.data_view import DataView  # noqa: E501
from mcenter_server_api.models.health_view import HealthView  # noqa: E501
from mcenter_server_api.models.report import Report  # noqa: E501
from mcenter_server_api import util


def dashboard_get():  # noqa: E501
    """Extract dashboard view

     # noqa: E501


    :rtype: List[Dashboard]
    """
    return 'do some magic!'


def data_science_report_get(ml_app_instance_id, workflow_node_id=None, agent_id=None, pipeline_id=None, start=None, end=None):  # noqa: E501
    """Extract data scientist view

     # noqa: E501

    :param ml_app_instance_id: MLApp instance identifier
    :type ml_app_instance_id: str
    :param workflow_node_id: workflow node id for which to fetch view
    :type workflow_node_id: str
    :param agent_id: agent identifier
    :type agent_id: str
    :param pipeline_id: Pipeline identifier
    :type pipeline_id: str
    :param start: start time from where to fetch health view
    :type start: int
    :param end: end time from where to fetch health view
    :type end: int

    :rtype: DataView
    """
    return 'do some magic!'


def health_report_get(ml_app_instance_id, start=None, end=None, pipeline_instance_a=None, pipeline_instance_b=None):  # noqa: E501
    """Extract health view

     # noqa: E501

    :param ml_app_instance_id: MLApp instance identifier
    :type ml_app_instance_id: str
    :param start: start time from where to fetch health view
    :type start: int
    :param end: end time from where to fetch health view
    :type end: int
    :param pipeline_instance_a: Pipeline instance identifier, of first node to compare
    :type pipeline_instance_a: str
    :param pipeline_instance_b: Pipeline instance identifier, of second node to compare
    :type pipeline_instance_b: str

    :rtype: HealthView
    """
    return 'do some magic!'


def reports_get():  # noqa: E501
    """Extract business report

     # noqa: E501


    :rtype: List[Report]
    """
    return 'do some magic!'
