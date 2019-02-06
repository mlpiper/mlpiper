import connexion
import six
import time

from mcenter_server_api import util


def db_query_get():  # noqa: E501
    """db_query_get

     # noqa: E501


    :rtype: None
    """
    return 'do some magic!'


def ml_app_instances_get():  # noqa: E501
    """ml_app_instances_get

     # noqa: E501


    :rtype: None
    """
    return 'do some magic!'


def ml_app_instances_ml_app_instance_id_archive_delete(ml_app_instance_id):  # noqa: E501
    """ml_app_instances_ml_app_instance_id_archive_delete

     # noqa: E501

    :param ml_app_instance_id: MLApp instance identifier
    :type ml_app_instance_id: str

    :rtype: None
    """
    return 'do some magic!'


def ml_app_instances_ml_app_instance_id_get(ml_app_instance_id):  # noqa: E501
    """ml_app_instances_ml_app_instance_id_get

     # noqa: E501

    :param ml_app_instance_id: MLApp instance identifier
    :type ml_app_instance_id: str

    :rtype: None
    """
    return 'do some magic!'


def ml_app_instances_ml_app_instance_id_logs_get(ml_app_instance_id):  # noqa: E501
    """ml_app_instances_ml_app_instance_id_logs_get

     # noqa: E501

    :param ml_app_instance_id: MLApp instance identifier
    :type ml_app_instance_id: str

    :rtype: None
    """
    return 'do some magic!'


def ml_app_instances_ml_app_instance_id_logs_prepare_post(ml_app_instance_id):  # noqa: E501
    """ml_app_instances_ml_app_instance_id_logs_prepare_post

     # noqa: E501

    :param ml_app_instance_id: MLApp instance identifier
    :type ml_app_instance_id: str

    :rtype: None
    """
    return 'do some magic!'


def ml_app_instances_ml_app_instance_id_logs_prepare_status_post(ml_app_instance_id):  # noqa: E501
    """ml_app_instances_ml_app_instance_id_logs_prepare_status_post

     # noqa: E501

    :param ml_app_instance_id: MLApp instance identifier
    :type ml_app_instance_id: str

    :rtype: None
    """
    return 'do some magic!'


def ml_app_instances_ml_app_instance_id_move_to_production_post(ml_app_instance_id):  # noqa: E501
    """ml_app_instances_ml_app_instance_id_move_to_production_post

     # noqa: E501

    :param ml_app_instance_id: MLApp instance identifier
    :type ml_app_instance_id: str

    :rtype: None
    """
    return 'do some magic!'


def ml_app_instances_ml_app_instance_id_set_model_post(ml_app_instance_id):  # noqa: E501
    """ml_app_instances_ml_app_instance_id_set_model_post

     # noqa: E501

    :param ml_app_instance_id: MLApp instance identifier
    :type ml_app_instance_id: str

    :rtype: None
    """
    return 'do some magic!'


def ml_app_instances_ml_app_instance_id_stop_delete(ml_app_instance_id):  # noqa: E501
    """ml_app_instances_ml_app_instance_id_stop_delete

     # noqa: E501

    :param ml_app_instance_id: MLApp instance identifier
    :type ml_app_instance_id: str

    :rtype: None
    """
    return 'do some magic!'


def ml_app_instances_ml_app_instance_id_thresholds_get(ml_app_instance_id):  # noqa: E501
    """ml_app_instances_ml_app_instance_id_thresholds_get

     # noqa: E501

    :param ml_app_instance_id: MLApp instance identifier
    :type ml_app_instance_id: str

    :rtype: None
    """
    return 'do some magic!'


def ml_app_instances_ml_app_instance_id_thresholds_post(ml_app_instance_id):  # noqa: E501
    """ml_app_instances_ml_app_instance_id_thresholds_post

     # noqa: E501

    :param ml_app_instance_id: MLApp instance identifier
    :type ml_app_instance_id: str

    :rtype: None
    """
    return 'do some magic!'


def ml_app_instances_post():  # noqa: E501
    """ml_app_instances_post

     # noqa: E501


    :rtype: None
    """
    return 'do some magic!'


def settings_build_manifest_get():  # noqa: E501
    """settings_build_manifest_get

     # noqa: E501


    :rtype: None
    """
    return dict(BuildType='Release',
                TimeCreated=time.strftime('%Y-%m-%d-%H-%M-%S'),
                Version='1.2.0',
                Build=1234,
                ProductName='MLOpsCenter')


def settings_global_parameters_get():  # noqa: E501
    """settings_global_parameters_get

     # noqa: E501


    :rtype: None
    """
    return {
        'auto.generate.default.users.password': 'true',
        'data_retention_period': '7 days',
        'eco.auth.ldap.group.base_dn': 'cn=mlops,dc=example,dc=org',
        'eco.auth.ldap.security_authentication': 'simple',
        'eco.auth.ldap.url': 'ldap://localhost:389',
        'eco.auth.ldap.user.base_dn': 'dc=example,dc=org',
        'eco.auth.ldap.user.cn_or_uid': 'uid',
        'eco.auth.mode': 'internal',
        'eco.communincation.protocol': 'http',
        'eco.dir': '/opt/parallelm/eco-dir',
        'eco.hadoop.auth.mode': 'default',
        'eco.internal_port': '4567',
        'eco.server.webport': '3456',
        'log.file': 'localhost-eco-server.log',
        'mlops.db.type': 'mysql',
        'mlops.user.repos.cloning.root': '/opt/parallelm/user-repos',
        'pipelines.path': '/opt/parallelm/pipelines',
        'pm.pipeline.path': '/opt/parallelm/algos.jar',
        'reflex.components.repo': '/opt/parallelm/components',
        'reflex.pipeline.components': '/opt/parallelm/components.json',
        'sql.database': 'mcenterdb',
        'sql.host': 'example.org',
        'sql.password': 'example',
        'sql.port': '3306',
        'sql.user': 'root',
        'ssl.cert.password': '123456',
        'ssl.keystore.file.path': '/opt/parallelm/mcenter.jks',
        'ssl.keystore.password': '123456',
        'zookeeper.host': '10.10.21.14:2181',
    }


def sourcecontrol_credentials_get():  # noqa: E501
    """sourcecontrol_credentials_get

     # noqa: E501


    :rtype: None
    """
    return 'do some magic!'


def sourcecontrol_credentials_name_delete(name):  # noqa: E501
    """sourcecontrol_credentials_name_delete

     # noqa: E501

    :param name: Source control credential name
    :type name: str

    :rtype: None
    """
    return 'do some magic!'


def sourcecontrol_credentials_name_get(name):  # noqa: E501
    """sourcecontrol_credentials_name_get

     # noqa: E501

    :param name: Source control credential name
    :type name: str

    :rtype: None
    """
    return 'do some magic!'


def sourcecontrol_credentials_post():  # noqa: E501
    """sourcecontrol_credentials_post

     # noqa: E501


    :rtype: None
    """
    return 'do some magic!'


def sourcecontrol_repos_repo_id_delete(repo_id):  # noqa: E501
    """sourcecontrol_repos_repo_id_delete

     # noqa: E501

    :param repo_id: Source control repository identifier
    :type repo_id: str

    :rtype: None
    """
    return 'do some magic!'


def sourcecontrol_repos_repo_id_get(repo_id):  # noqa: E501
    """sourcecontrol_repos_repo_id_get

     # noqa: E501

    :param repo_id: Source control repository identifier
    :type repo_id: str

    :rtype: None
    """
    return 'do some magic!'


def sourcecontrol_repos_repo_id_put(repo_id):  # noqa: E501
    """sourcecontrol_repos_repo_id_put

     # noqa: E501

    :param repo_id: Source control repository identifier
    :type repo_id: str

    :rtype: None
    """
    return 'do some magic!'


def sourcecontrol_repos_repo_id_refs_get(repo_id):  # noqa: E501
    """sourcecontrol_repos_repo_id_refs_get

     # noqa: E501

    :param repo_id: Source control repository identifier
    :type repo_id: str

    :rtype: None
    """
    return 'do some magic!'


def stats_get():  # noqa: E501
    """stats_get

     # noqa: E501


    :rtype: None
    """
    return 'do some magic!'
