
import os
import requests_mock

from parallelm.mlops.config_info import ConfigInfo
from parallelm.mlops.mlops_ctx import MLOpsCtx
from parallelm.mlops.mlops_mode import MLOpsMode
from parallelm.mlops.mlops_env_constants import MLOpsEnvConstants
from parallelm.mlops.mlops_rest_factory import MlOpsRestFactory


class TimeCapture1:

    attribute_names_list1 =['distance', 'pressure', 'altitude', 'temperature', 'PredictionHistogram',
                           'pipelinestat.averageDistanceToClusters', 'dataheatmap',
                           'pipelinestat.count', 'pipelinestat.WSSER',
                           'modelstats.distanceMatrixStat']

    count = [[30000], [60000], [90000], [120000], [150000], [180000], [210000], [240000], [270000],
             [300000], [330000]]

    mlapp_id = "8b6e1602-bb53-4e54-bd17-26cf6402ff36"

    mlapp_policy = "ALWAYS_UPDATE"

    mlapp_nodes = [['b9e53ad3-590d-4a40-8994-9c313e7f56cb', 'MODEL_PRODUCER', 'sprml train'],
                   ['c88c8fba-1d73-4601-8fb1-67bbb6359139', 'MODEL_CONSUMER', 'sparkML Inference']]

    events = ['GenericEvent', 'GenericEvent', 'GenericEvent', 'Model', 'GenericEvent',
              'GenericEvent', 'ModelAccepted', 'GenericEvent', 'GenericEvent', 'GenericEvent',
              'ModelAccepted', 'GenericEvent', 'Model', 'GenericEvent', 'GenericEvent']

    file_names = ['mlopsdb-aggregates-6064a0b6-f006-4855-a941-6da55ef3c917.csv',
                  'mlopsdb-Ion-stats-6064a0b6-f006-4855-a941-6da55ef3c917-Instance-b9e53ad3-590d-4a40-8994-9c313e7f56cb-Agent-daenerys-c18.csv_matrix.csv',
                  'mlopsdb-Ion-stats-6064a0b6-f006-4855-a941-6da55ef3c917-Instance-c88c8fba-1d73-4601-8fb1-67bbb6359139-Agent-daenerys-c18.csv_heatmap.csv',
                  'mlopsdb-Ion-stats-6064a0b6-f006-4855-a941-6da55ef3c917-Instance-c88c8fba-1d73-4601-8fb1-67bbb6359139-Agent-daenerys-c18.csv', 'MLApp-details.txt',
                  'mlopsdb-Ion-sysstats-6064a0b6-f006-4855-a941-6da55ef3c917-Instance-b9e53ad3-590d-4a40-8994-9c313e7f56cb-Agent-daenerys-c18.csv',
                  'mlopsdb-Ion-stats-6064a0b6-f006-4855-a941-6da55ef3c917-Instance-c88c8fba-1d73-4601-8fb1-67bbb6359139-Agent-daenerys-c18.csv_multilinegraph.csv',
                  'mlopsdb-Ion-stats-6064a0b6-f006-4855-a941-6da55ef3c917-Instance-b9e53ad3-590d-4a40-8994-9c313e7f56cb-Agent-daenerys-c18.csv_bargraph.csv',
                  'mlopsdb-Ion-stats-6064a0b6-f006-4855-a941-6da55ef3c917-Instance-c88c8fba-1d73-4601-8fb1-67bbb6359139-Agent-daenerys-c18.csv_bargraph.csv',
                  'mlopsdb-Ion-stats-6064a0b6-f006-4855-a941-6da55ef3c917-Instance-b9e53ad3-590d-4a40-8994-9c313e7f56cb-Agent-daenerys-c18.csv_multilinegraph.csv',
                  'mlopsdb-events-6064a0b6-f006-4855-a941-6da55ef3c917.csv',
                  'mlopsdb-Ion-stats-6064a0b6-f006-4855-a941-6da55ef3c917-Instance-b9e53ad3-590d-4a40-8994-9c313e7f56cb-Agent-daenerys-c18.csv',
                  'mlopsdb-Ion-sysstats-6064a0b6-f006-4855-a941-6da55ef3c917-Instance-c88c8fba-1d73-4601-8fb1-67bbb6359139-Agent-daenerys-c18.csv']

    original_file_names = ['mlopsdb-aggregates-6064a0b6-f006-4855-a941-6da55ef3c917.csv',
                  'mlopsdb-Ion-stats-6064a0b6-f006-4855-a941-6da55ef3c917-Instance-c88c8fba-1d73-4601-8fb1-67bbb6359139-Agent-daenerys-c18.csv', 'MLApp-details.txt',
                  'mlopsdb-Ion-sysstats-6064a0b6-f006-4855-a941-6da55ef3c917-Instance-b9e53ad3-590d-4a40-8994-9c313e7f56cb-Agent-daenerys-c18.csv',
                  'mlopsdb-events-6064a0b6-f006-4855-a941-6da55ef3c917.csv',
                  'mlopsdb-Ion-stats-6064a0b6-f006-4855-a941-6da55ef3c917-Instance-b9e53ad3-590d-4a40-8994-9c313e7f56cb-Agent-daenerys-c18.csv',
                  'mlopsdb-Ion-sysstats-6064a0b6-f006-4855-a941-6da55ef3c917-Instance-c88c8fba-1d73-4601-8fb1-67bbb6359139-Agent-daenerys-c18.csv']

    bins = ['-inf to -25.9763', '-25.9763 to -20.7824', '-20.7824 to -15.5885',
             '-15.5885 to -10.3945', '-10.3945 to -5.2006', '-5.2006 to -0.0067',
             '-0.0067 to 5.1872', '5.1872 to 10.3811', '10.3811 to 15.5751', '15.5751 to 20.769',
             '20.769 to 25.9629', '25.9629 to +inf']

    bins_vert = ['-inf\nto\n-25.9763', '-25.9763\nto\n-20.7824', '-20.7824\nto\n-15.5885',
                  '-15.5885\nto\n-10.3945', '-10.3945\nto\n-5.2006', '-5.2006\nto\n-0.0067',
                  '-0.0067\nto\n5.1872', '5.1872\nto\n10.3811', '10.3811\nto\n15.5751',
                  '15.5751\nto\n20.769', '20.769\nto\n25.9629', '25.9629\nto\n+inf']
