import connexion
import six

from mcenter_server_api.models.repo import Repo  # noqa: E501
from mcenter_server_api import util


def sourcecontrol_repos_get():  # noqa: E501
    """Get list of all registered repositories

     # noqa: E501


    :rtype: List[Repo]
    """
    return 'do some magic!'


def sourcecontrol_repos_post(repo):  # noqa: E501
    """Register a repository

     # noqa: E501

    :param repo: Repository details
    :type repo: dict | bytes

    :rtype: Repo
    """
    if connexion.request.is_json:
        repo = Repo.from_dict(connexion.request.get_json())  # noqa: E501
    return 'do some magic!'
