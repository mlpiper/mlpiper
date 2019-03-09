
import os
import requests_mock

from parallelm.mlops.config_info import ConfigInfo
from parallelm.mlops.mlops_ctx import MLOpsCtx
from parallelm.mlops.mlops_mode import MLOpsMode
from parallelm.mlops.mlops_env_constants import MLOpsEnvConstants
from parallelm.mlops.mlops_rest_factory import MlOpsRestFactory


class ION1:

    ION_ID = "ion-id-0"
    ION_NAME = "ion-name"
    ION_INSTANCE_ID = 'ion-instance-id-0'
    ION_PATTERN_ID = 'ion-pattern-id-0'

    NODE_0_ID = "0"
    NODE_1_ID = "1"

    PIPELINE_PATTERN_ID_0 = "pipeline-pattern-id-0"
    PIPELINE_PATTERN_ID_1 = "pipeline-pattern-id-1"

    PIPELINE_PROFILE_ID_0 = "pipeline-pattern-id-0"
    PIPELINE_PROFILE_ID_1 = "pipeline-pattern-id-1"

    PIPELINE_ID_0 = "pipeline-id-0"
    PIPELINE_ID_1 = "pipeline-id-1"

    PIPELINE_INST_ID_0 = "pipeline-instance-id-0"
    PIPELINE_INST_ID_1 = "pipeline-instance-id-1"

    MODEL_ID = "model-id-1"

    AGENT_ID_0 = "agent-id-0"
    EE_ID_0 = "ee-id-0"

    TOKEN = "token_token_token"


test_workflow_instances = {
    "id": ION1.ION_ID,
    "workflow": {
        "ionPatternId": ION1.ION_PATTERN_ID,
        "ionPatternName": ION1.ION_NAME,
        "ionInstanceIds": [
            ION1.ION_INSTANCE_ID
        ],
        "isProfile": True,
        "version": "1",
        "workflow": [
            {
                "id": ION1.NODE_0_ID,
                "pipelineMode": "offline",
                "pipelineType": "model_producer",
                "pipelineEETuple": {
                    "pipelineProfileId": ION1.PIPELINE_PATTERN_ID_0,
                    "executionEnvironment": {
                        "description": "",
                        "agentId": ION1.AGENT_ID_0,
                        "url": "",
                        "configs": {
                            "engConfig": {
                                "type": "generic",
                                "arguments": {
                                    "log-level": {"value": "info"}
                                }
                            },
                            "rmConfig": {
                                "type": "os",
                                "arguments": {
                                    "tensorflow.ld_library_path": {"value": ""},
                                    "tensorflow.path": {"value": ""},
                                    "tensorflow.python": {"value": "/usr/bin/python"}
                                }
                            }
                        },
                        "id": ION1.EE_ID_0,
                        "name": "EE1",
                        "created": 0,
                        "createdBy": "admin"
                    },
                    "pipeline": {
                        "pipeline": "{\"engineType\": \"SparkBatch\", \"name\": \"training_kmeans_spark-profile-localhost\", \"pipe\": [{\"type\": \"CsvToDF\", \"id\": 0, \"name\": \"CsvToDF\", \"arguments\": {\"seperator\": \",\", \"withHeaders\": false, \"filepath\": \"\/data-lake\/jenkins-data\/GE_DEMO\/Train_dataset_A.csv\"}, \"parents\": []}, {\"type\": \"VectorAssemblerComponent\", \"parents\": [{\"output\": 0, \"input\": 0, \"parent\": 0}], \"id\": 1, \"name\": \"VectorAssemblerComponent\", \"arguments\": {\"outputCol\": \"features\"}}, {\"type\": \"ReflexKmeansML\", \"parents\": [{\"output\": 0, \"input\": 0, \"parent\": 1}], \"id\": 2, \"name\": \"ReflexKmeansML\", \"arguments\": {\"tol\": 1e-05, \"maxIter\": 400, \"k\": 3, \"featuresCol\": \"features\", \"initStep\": 2, \"seed\": 2, \"predictionCol\": \"predictions\", \"initMode\": \"k-means||\"}}]}",
                        "isProfile": True,
                        "version": "1",
                        "engineType": "SparkBatch",
                        "profileIdDependencySet": [
                            "cb9411ce-d103-4389-bcd1-541e7e45185e"
                        ],
                        "isVisible": True,
                        "defaultModel": "",
                        "isUserDefined": False,
                        "id": "4da92113-d85e-46e2-8609-c4b9cfe607f5",
                        "name": "training_kmeans_spark-profile-localhost",
                        "created": 1529945512839,
                        "createdBy": "admin"
                    },
                    "agent": {
                        "address": "localhost",
                        "eeIds": [
                            ION1.EE_ID_0
                        ],
                        "state": "OFFLINE",
                        "id": ION1.AGENT_ID_0,
                        "name": "localhost",
                        "created": 1529945502375,
                        "createdBy": "admin"
                    }
                },
                "pipelinePatternId": ION1.PIPELINE_PATTERN_ID_0,
                "parent": "-1",
                "children": "2",
                "parallelism": 1,
                "restServerPort": 12121,
                "isVisible": True,
                "cronSchedule": "0 0\/10 * * * ?"
            },
            {
                "id": ION1.NODE_1_ID,
                "pipelineMode": "online",
                "pipelineType": "model_consumer",
                "pipelineEETuple": {
                    "pipelineProfileId": ION1.PIPELINE_PATTERN_ID_1,
                    "executionEnvironment": {
                        "description": "",
                        "agentId": ION1.AGENT_ID_0,
                        "url": "",
                        "configs": {
                            "engConfig": {
                                "type": "generic",
                                "arguments": {
                                    "log-level": {"value": "info"}
                                }
                            },
                            "rmConfig": {
                                "type": "os",
                                "arguments": {
                                    "tensorflow.ld_library_path": {"value": ""},
                                    "tensorflow.path": {"value": ""},
                                    "tensorflow.python": {"value": "/usr/bin/python"}
                                }
                            }
                        },
                        "id": ION1.EE_ID_0,
                        "name": "EE1",
                        "created": 0,
                        "createdBy": "admin"
                    },
                    "pipeline": {
                        "pipeline": "{\"engineType\": \"FlinkStreaming\", \"name\": \"inference_kmeans_flink-profile-localhost\", \"pipe\": [{\"type\": \"ReflexKafkaConnector\", \"id\": 1, \"name\": \"Turbine 1\", \"arguments\": {\"host\": \"localhost\", \"port\": 9092, \"topic\": \"KmeansAnomaly\"}, \"parents\": []}, {\"type\": \"ReflexStringToLabeledVectorComponent\", \"id\": 2, \"name\": \"String To Labeled Vector\", \"arguments\": {\"indicesRange\": \"0-9\", \"attributes\": 10}, \"parents\": [{\"output\": 0, \"parent\": 1}]}, {\"type\": \"KmeansAnomalyComponent\", \"id\": 3, \"name\": \"KmeansAnomalyComponent\", \"arguments\": {\"enableValidation\": false, \"enablePerformance\": true, \"centroidCount\": 3, \"anomalyDistanceThreshold\": 70, \"attributes\": 10}, \"parents\": [{\"output\": 0, \"parent\": 2}]}]}",
                        "isProfile": True,
                        "version": "1",
                        "engineType": "FlinkStreaming",
                        "profileIdDependencySet": [
                            "cb9411ce-d103-4389-bcd1-541e7e45185e"
                        ],
                        "isVisible": True,
                        "defaultModel": "",
                        "isUserDefined": False,
                        "id": "098730d5-f9f5-46f9-a8ea-f533c1f6b35a",
                        "name": "inference_kmeans_flink-profile-localhost",
                        "created": 1529945514409,
                        "createdBy": "admin"
                    },
                    "agent": {
                        "address": "localhost",
                        "eeIds": [ION1.EE_ID_0],
                        "state": "OFFLINE",
                        "id": ION1.AGENT_ID_0,
                        "name": "localhost",
                        "created": 1529945502375,
                        "createdBy": "admin"
                    }
                },
                "pipelinePatternId": ION1.PIPELINE_PATTERN_ID_1,
                "parent": "1",
                "children": "-1",
                "parallelism": 1,
                "restServerPort": 12121,
                "isVisible": True
            }
        ],
        "modelPolicy": "ALWAYS_UPDATE",
        "globalThreshold": 0.8,
        "canaryThreshold": -1,
        "enableHealth": True,
        "runNow": True,
        "comparisonWorkflowNodeSets": [

        ],
        "id": "cb9411ce-d103-4389-bcd1-541e7e45185e",
        "name": ION1.ION_NAME,
        "created": 1529945515454,
        "createdBy": "admin"
    },
    "status": "MODEL_READY",
    "operationMode": "SANDBOX",
    "info": [

    ],
    "pipelineInstances": [
        {
            "pipeline": "{\"engineType\": \"SparkBatch\", \"name\": \"training_kmeans_spark-profile-localhost\", \"pipe\": [{\"type\": \"CsvToDF\", \"id\": 0, \"name\": \"CsvToDF\", \"arguments\": {\"seperator\": \",\", \"withHeaders\": false, \"filepath\": \"\/data-lake\/jenkins-data\/GE_DEMO\/Train_dataset_A.csv\"}, \"parents\": []}, {\"type\": \"VectorAssemblerComponent\", \"parents\": [{\"output\": 0, \"input\": 0, \"parent\": 0}], \"id\": 1, \"name\": \"VectorAssemblerComponent\", \"arguments\": {\"outputCol\": \"features\"}}, {\"type\": \"ReflexKmeansML\", \"parents\": [{\"output\": 0, \"input\": 0, \"parent\": 1}], \"id\": 2, \"name\": \"ReflexKmeansML\", \"arguments\": {\"tol\": 1e-05, \"maxIter\": 400, \"k\": 3, \"featuresCol\": \"features\", \"initStep\": 2, \"seed\": 2, \"predictionCol\": \"predictions\", \"initMode\": \"k-means||\"}}]}",
            "pipelineMode": "OFFLINE",
            "pipelineType": "MODEL_PRODUCER",
            "hostname": "localhost",
            "agentId": ION1.AGENT_ID_0,
            "ecoJobId": "fc8a4901-b2e0-440c-aed2-04d168d06b93",
            "platform": "SPARK",
            "parallelism": 1,
            "cronSchedule": "0 0\/10 * * * ?",
            "pipelineInstanceId": ION1.PIPELINE_INST_ID_0,
            "pipelineId": ION1.PIPELINE_PROFILE_ID_0,
            "pipelineName": "training_kmeans_spark-profile-localhost",
            "defaultModelId": "",
            "codeGenRestServerPort": 0,
            "workflowNodeId": ION1.NODE_0_ID,
            "pipelineSystemParams": {
                "statsDBHost": "localhost",
                "statsDBPort": 8086,
                "statsMeasurementID": "predictions-fc8a4901-b2e0-440c-aed2-04d168d06b93",
                "mlObjectSocketHost": "",
                "mlObjectSocketSourcePort": 0,
                "mlObjectSocketSinkPort": 0,
                "modelFileSinkPath": "",
                "healthStatFilePath": "",
                "workflowInstanceId": "",
                "socketSourcePort": 0,
                "socketSinkPort": 0,
                "enableHealth": True,
                "canaryThreshold": -1
            },
            "isUserDefined": False,
            "runNow": True,
            "packedCodeComponentPath": [
                "\/opt\/sriram\/owl\/mlops\/pm-eco-1.0.0\/..\/pm-algorithms\/ReflexAlgos-jar-with-dependencies.jar"
            ],
            "token": "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1NjE0ODE1MjUsInVzZXJuYW1lIjoiYWRtaW4ifQ.9mLi05I-Lki1OhhiukJxCY6BdeUrH1lI33qtzjyg9LU",
            "enableHealth": True,
            "serviceUser": {
                "username": "admin",
                "password": "$2a$12$7J8PRLc5COydL5kBtoia6e8cFTCIiw509rb3jaZzmGNGAEHWwtxqO",
                "firstName": "admin",
                "lastName": "admin",
                "avatarURL": "",
                "email": "",
                "contactNo": "",
                "accountType": "user",
                "roleIds": [
                    "9c29d247-f911-4e0c-85c6-d9a5897ede5d"
                ],
                "authMode": "internal",
                "id": "e03ea588-f554-4178-b677-ccad54b82633",
                "name": "admin",
                "created": 1529944745786,
                "createdBy": "admin"
            },
            "agent": {
                "address": "localhost",
                "state": "OFFLINE",
                "id": ION1.AGENT_ID_0,
                'eeIds': [ION1.EE_ID_0],
                "name": "localhost",
                "created": 1529945502375,
                "createdBy": "admin"
            }
        },
        {
            "pipeline": "{\"engineType\": \"FlinkStreaming\", \"name\": \"inference_kmeans_flink-profile-localhost\", \"pipe\": [{\"type\": \"ReflexKafkaConnector\", \"id\": 1, \"name\": \"Turbine 1\", \"arguments\": {\"host\": \"localhost\", \"port\": 9092, \"topic\": \"KmeansAnomaly\"}, \"parents\": []}, {\"type\": \"ReflexStringToLabeledVectorComponent\", \"id\": 2, \"name\": \"String To Labeled Vector\", \"arguments\": {\"indicesRange\": \"0-9\", \"attributes\": 10}, \"parents\": [{\"output\": 0, \"parent\": 1}]}, {\"type\": \"KmeansAnomalyComponent\", \"id\": 3, \"name\": \"KmeansAnomalyComponent\", \"arguments\": {\"enableValidation\": false, \"enablePerformance\": true, \"centroidCount\": 3, \"anomalyDistanceThreshold\": 70, \"attributes\": 10}, \"parents\": [{\"output\": 0, \"parent\": 2}]}]}",
            "pipelineMode": "ONLINE",
            "pipelineType": "MODEL_CONSUMER",
            "hostname": "localhost",
            "agentId": ION1.AGENT_ID_0,
            "ecoJobId": "fc8a4901-b2e0-440c-aed2-04d168d06b93",
            "platform": "PM",
            "parallelism": 1,
            "pipelineInstanceId": ION1.PIPELINE_INST_ID_1,
            "pipelineId": ION1.PIPELINE_PROFILE_ID_1,
            "pipelineName": "inference_kmeans_flink-profile-localhost",
            "defaultModelId": "",
            "codeGenRestServerPort": 0,
            "workflowNodeId": ION1.NODE_1_ID,
            "pipelineSystemParams": {
                "statsDBHost": "localhost",
                "statsDBPort": 8086,
                "statsMeasurementID": "predictions-fc8a4901-b2e0-440c-aed2-04d168d06b93",
                "mlObjectSocketHost": "",
                "mlObjectSocketSourcePort": 0,
                "mlObjectSocketSinkPort": 0,
                "modelFileSinkPath": "",
                "healthStatFilePath": "",
                "workflowInstanceId": "",
                "socketSourcePort": 0,
                "socketSinkPort": 0,
                "enableHealth": True,
                "canaryThreshold": -1
            },
            "isUserDefined": False,
            "runNow": True,
            "packedCodeComponentPath": [
                "\/opt\/sriram\/owl\/mlops\/pm-eco-1.0.0\/..\/pm-algorithms\/ReflexAlgos-jar-with-dependencies.jar"
            ],
            "token": "eyJhbGciOiJIUzI1NiJ9.eyJleHAiOjE1NjE0ODE1MjUsInVzZXJuYW1lIjoiYWRtaW4ifQ.9mLi05I-Lki1OhhiukJxCY6BdeUrH1lI33qtzjyg9LU",
            "enableHealth": True,
            "serviceUser": {
                "username": "admin",
                "password": "$2a$12$7J8PRLc5COydL5kBtoia6e8cFTCIiw509rb3jaZzmGNGAEHWwtxqO",
                "firstName": "admin",
                "lastName": "admin",
                "avatarURL": "",
                "email": "",
                "contactNo": "",
                "accountType": "user",
                "roleIds": [
                    "9c29d247-f911-4e0c-85c6-d9a5897ede5d"
                ],
                "authMode": "internal",
                "id": "e03ea588-f554-4178-b677-ccad54b82633",
                "name": "admin",
                "created": 1529944745786,
                "createdBy": "admin"
            },
            "agent": {
                "address": "localhost",
                "state": "OFFLINE",
                "id": ION1.AGENT_ID_0,
                'eeIds': [ION1.EE_ID_0],
                "name": "localhost",
                "created": 1529945501824,
                "createdBy": "admin"
            }
        }
    ],
    "pipelineInstanceIdToWfNodeId": {
        ION1.PIPELINE_INST_ID_0: ION1.NODE_0_ID,
        ION1.PIPELINE_INST_ID_1: ION1.NODE_1_ID
    },
    "createdBy": "admin",
    "token": ION1.TOKEN,
    "isVisible": True,
    "globalThreshold": 0.8,
    "canaryThreshold": -1,
    "thresholdMap": {

    },
    "launchTime": 1529945525916,
    "serviceUser": {
        "username": "admin",
        "password": "$2a$12$7J8PRLc5COydL5kBtoia6e8cFTCIiw509rb3jaZzmGNGAEHWwtxqO",
        "firstName": "admin",
        "lastName": "admin",
        "avatarURL": "",
        "email": "",
        "contactNo": "",
        "accountType": "user",
        "roleIds": [
            "9c29d247-f911-4e0c-85c6-d9a5897ede5d"
        ],
        "authMode": "internal",
        "id": "e03ea588-f554-4178-b677-ccad54b82633",
        "name": "admin",
        "created": 1529944745786,
        "createdBy": "admin"
    }
}

test_ee_info = [{
    "id": ION1.EE_ID_0,
    "name": "EE1",
    "created": 0,
    "createdBy": "admin",
    "description": "",
    "agentId": ION1.AGENT_ID_0,
    "url": "",
    "configs": {
        "engConfig": {
            "type": "generic",
            "arguments": {
                "log-level": {"value": "info"}
            }
        },
        "rmConfig": {
            "type": "os",
            "arguments": {
                "tensorflow.ld_library_path": {"value": ""},
                "tensorflow.path": {"value": ""},
                "tensorflow.python": {"value": "/usr/bin/python"}
            }
        }
    }
}]

test_agents_info = [{
    'name': u'localhost',
    'created': 1520284711230,
    'state': u'ONLINE',
    'createdBy': u'admin',
    'eeIds': [ION1.EE_ID_0],
    'address': u'localhost',
    'id': ION1.AGENT_ID_0
}]

test_models_info = [
    {'ionName': 'mlops-tests',
     'raiseAlert': False,
     'sequence': 4,
     'eventType': 'Model',
     'clearedTimestamp': 0,
     'id': ION1.MODEL_ID,
     'state': 'APPROVED',
     'msgType': 'UNKNOWN',
     'type': 'Model',
     'modelId': '8c95deaf-87e4-4c21-bc92-e5b1a0454f9a',
     'pipelineInstanceId': ION1.PIPELINE_INST_ID_0,
     'format': 'TEXT',
     'deletedTimestamp': 0,
     'createdTimestamp': 1518460283900,
     'host': 'localhost',
     'modelSize': 0,
     'name': 'model-4',
     'stateDescription': '',
     'modelFormat': 'TEXT',
     'created': 1518460765161,
     'workflowRunId': '13445bb4-535a-4d45-b2f2-77293026e3da',
     'reviewedBy': ''
     },
    {'ionName': 'mlops-tests',
     'raiseAlert': False,
     'sequence': 8,
     u'eventType': 'Model',
     u'clearedTimestamp': 0,
     'id': u'9d1d4a81-29a0-492f-a6c7-d35489250368',
     'state': u'APPROVED',
     'msgType': u'UNKNOWN',
     'type': u'Model',
     'modelId': u'9d1d4a81-29a0-492f-a6c7-d35489250368',
     'pipelineInstanceId': ION1.PIPELINE_INST_ID_0,
     'format': u'PMML',
     'deletedTimestamp': 0,
     'createdTimestamp': 1518460573573,
     'host': u'localhost',
     'modelSize': 0,
     'name': u'model-8',
     'stateDescription': '',
     'modelFormat': u'PMML',
     'created': 1518460765161,
     'workflowRunId': u'bdc2ee10-767c-4524-ba72-8268a3894bff',
     'reviewedBy': u''},
]

test_health_info = {
  "globalThreshold": 0.2,
  "attributes": [
  ],
  "canaryThreshold": 0.5,
  "thresholdType": "ALL"
}

def set_mlops_env(ion_id, ion_node_id, eco_host="localhost", eco_port="3456", token="token_token_token", model_id=None):
    """
    Helper for setting the environment variables required by mlops to function
    :param ion_id:
    :param ion_node:
    :param eco_host:
    :param eco_port:
    :param token:
    :param model_id:
    :return:
    """
    os.environ[MLOpsEnvConstants.MLOPS_ION_ID] = ion_id
    os.environ[MLOpsEnvConstants.MLOPS_ION_NODE_ID] = ion_node_id
    os.environ[MLOpsEnvConstants.MLOPS_DATA_REST_SERVER] = eco_host
    os.environ[MLOpsEnvConstants.MLOPS_DATA_REST_PORT] = eco_port
    os.environ[MLOpsEnvConstants.MLOPS_TOKEN] = token
    if model_id:
        os.environ[MLOpsEnvConstants.MLOPS_MODEL_ID] = model_id


def build_ion_ctx():
    ion_instance_id = ION1.ION_ID
    ion_node_id = ION1.NODE_1_ID
    token = "token_token_token"

    set_mlops_env(ion_id=ion_instance_id, ion_node_id=ion_node_id, token=token)
    rest_helper = MlOpsRestFactory().get_rest_helper(MLOpsMode.AGENT, mlops_server="localhost", mlops_port="3456", token=token)

    with requests_mock.mock() as m:
        m.get(rest_helper.url_get_workflow_instance(ion_instance_id), json=test_workflow_instances)
        m.get(rest_helper.url_get_ees(), json=test_ee_info)
        m.get(rest_helper.url_get_agents(), json=test_agents_info)
        m.get(rest_helper.url_get_health_thresholds(ion_instance_id), json=test_health_info)

        ci = ConfigInfo().read_from_env()
        mlops_ctx = MLOpsCtx(config=ci, mode=MLOpsMode.ATTACH)
        return mlops_ctx
