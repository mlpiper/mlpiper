import pandas as pd
import numpy as np
from parallelm.mlops.data_frame import DataFrameHelper
import pytest
from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats.health.general_hist_stat import GeneralHistogramStat
import ast
import json

# Pandas DataFrame equals method compares content down to each character, number of columns, data type of column names
# and each row

def _check_data_frames(data, json, is_positive_test):
    pandas_data_frame = pd.DataFrame(data=data)
    data_frame_helper = DataFrameHelper()
    mlops_data_frame = data_frame_helper.create_data_frame(json)

    if is_positive_test:
        pd.testing.assert_frame_equal(left=pandas_data_frame, right=mlops_data_frame, check_like=True)
    else:
        is_different_frame = False
        if not mlops_data_frame.equals(pandas_data_frame):
            is_different_frame = True

        assert is_different_frame

def test_data_frame_accuracy():
    # Positive tests
    # LINEGRAPH
    data_frame_helper = DataFrameHelper()
    linegraph_data = {"value": [1.0, 2.0], "time" : ['2018-02-08T18:27:27.183Z', '2018-02-08T18:27:27.187Z']}

    linegraph_json = {
        "graphType": "LINEGRAPH",
        "values": [
            {
                "name": "stat",
                "columns": [
                    "time",
                    "value"
                ],
                "values": [
                    [
                        "2018-02-08T18:27:27.183Z",
                        "{\"0\":1.0}"
                    ],
                    [
                        "2018-02-08T18:27:27.187Z",
                        "{\"0\":2.0}"
                    ]
                ]
            }
        ]
    }
    _check_data_frames(linegraph_data, linegraph_json, True)

    # BARGRAPH
    bargraph_data = {"time": ["2018-02-08T18:27:28.273Z", "2018-02-08T18:27:29.273Z"],
                     "aa": [10,11], "bb": [15,16], "cc": [12,13], "dd": [9,10], "ee": [8,9]}

    bargraph_json = {
        "graphType": "BARGRAPH",
        "values": [
            {
                "name": "stat",
                "columns": [
                    "time",
                    "value"
                ],
                "values": [
                    [
                        "2018-02-08T18:27:28.273Z",
                        "{\"bar\": \"{\\\"aa\\\": 10,\\\"bb\\\": 15,\\\"cc\\\": 12,\\\"dd\\\": 9,\\\"ee\\\": 8}\"}"
                    ],
                    [
                        "2018-02-08T18:27:29.273Z",
                        "{\"bar\": \"{\\\"aa\\\": 11,\\\"bb\\\": 16,\\\"cc\\\": 13,\\\"dd\\\": 10,\\\"ee\\\": 9}\"}"
                    ]
                ]
            }
        ]
    }

    _check_data_frames(bargraph_data, bargraph_json, True)

    # MULTILINEGRAPH
    multilinegraph_data = {"time": ["2018-02-08T18:27:28.262Z", "2018-01-08T18:27:28.262Z"],
                           "l2": [15,14], "l3": [22,21], "l1": [0,0]}

    multilinegraph_json = {
        "graphType": "MULTILINEGRAPH",
        "values": [
            {
                "name": "stat",
                "columns": [
                    "time",
                    "value"
                ],
                "values": [
                    [
                        "2018-02-08T18:27:28.262Z",
                        "{\"l2\": 15, \"l3\": 22, \"l1\": 0}"
                    ],
                    [
                        "2018-01-08T18:27:28.262Z",
                        "{\"l2\": 14, \"l3\": 21, \"l1\": 0}"
                    ]
                ]
            }
        ]
    }
    _check_data_frames(multilinegraph_data, multilinegraph_json, True)

    # HISTOGRAM
    # continuous data
    continuous_data = {'hist_values': [[0.0, 10.0, 99.0, 534.0], [0.0, 8.0, 100.0, 569.0], [0.0, 10.0, 93.0, 526.0],
                                [0.0, 9.0, 105.0, 9.0]],
                       'bin_edges': [['-2.147483648E9', '-24.4212', '-23.20814', '-21.995079999999998', '20.78202'],
                                     ['-2.147483648E9', '-24.0783', '-22.96705', '-21.8558', '20.74455'],
                                     ['-2.147483648E9', '-24.4212', '-23.20814', '-21.995079999999998', '20.78202'],
                                     ['-2.147483648E9', '-24.0783', '-22.96705', '-21.8558', '12.9658']],
                       'hist_type': ['continuous', 'continuous', 'continuous', 'continuous'],
                       'stat_name': ['stat', 'stat', 'stat', 'stat'],
                       'time': ['2018-02-16T22:52:25.633Z', '2018-02-16T22:52:26.224Z', '2018-02-16T22:52:29.452Z',
                                '2018-02-16T22:52:31.043Z']}

    continuous_json = {'category': 'INSTANT',
                       'graphType':
                           'BARGRAPH',
                       'values': [{'values': [
                           ['2018-02-16T22:52:25.633Z',
                            '{"0":[{"-2.147483648E9 to -24.4212":0.0},'
                            '{"-24.4212 to -23.20814":10.0},'
                            '{"-23.20814 to -21.995079999999998":99.0},'
                            '{"-21.995079999999998 to 20.78202":534.0}]}'],
                           ['2018-02-16T22:52:26.224Z',
                            '{"0":[{"-2.147483648E9 to -24.0783":0.0},'
                            '{"-24.0783 to -22.96705":8.0},'
                            '{"-22.96705 to -21.8558":100.0},'
                            '{"-21.8558 to 20.74455":569.0}]}'],
                           ['2018-02-16T22:52:29.452Z',
                            '{"0":[{"-2.147483648E9 to -24.4212":0.0},'
                            '{"-24.4212 to -23.20814":10.0},'
                            '{"-23.20814 to -21.995079999999998":93.0},'
                            '{"-21.995079999999998 to 20.78202":526.0}]}'],
                           ['2018-02-16T22:52:31.043Z',
                            '{"0":[{"-2.147483648E9 to -24.0783":0.0},'
                            '{"-24.0783 to -22.96705":9.0},'
                            '{"-22.96705 to -21.8558":105.0},'
                            '{"-21.8558 to 12.9658":9.0}]}']],
                           'name': 'stat',
                           'columns': ['time', 'value']
                       }],
                       'name': 'PredictionHistogram',
                       'id': '5c81dd4b-4302-4d68-b6b8-be0ee14a0d95'}

    _check_data_frames(continuous_data, continuous_json, True)

    categorical_data= {'hist_values': [[0.9167, 0.0014, 0.0039, 0.0012, 0.0015, 0.0057, 0.0014, 0.0069, 0.0014, 0.0123,
                                  0.0021, 0.0023, 0.0386]],
                        'bin_edges': [['42064', '42086', '42102', '42106', '42112', '42114', '42128', '42134',
                                       '42146', '42150', '42166', '42180', '42184']],
                        'hist_type': ['categorical'],
                        'stat_name': ['stat'],
                        'time': [1519325754]}

    categorical_json = {'category': 'INSTANT',
                        'graphType':
                            'BARGRAPH',
                        'values': [{"values": [
                            [
                                1519325754,
                                "{\"campaign_id\":[{\"42064\":0.9167},{\"42086\":0.0014},{\"42102\":0.0039},"
                                "{\"42106\":0.0012},{\"42112\":0.0015},{\"42114\":0.0057},{\"42128\":0.0014},"
                                "{\"42134\":0.0069},{\"42146\":0.0014},{\"42150\":0.0123},{\"42166\":0.0021},"
                                "{\"42180\":0.0023},{\"42184\":0.0386}]}"
                            ]],
                            'name': 'stat',
                            'columns': ['time', 'value']
                        }],
                        'name': 'CategoricalHistogram',
                        'id': '5c81dd4b-4302-4d68-b6b8-be0ee14a0d95'
                        }

    _check_data_frames(categorical_data, categorical_json, True)


    # Matrix
    matrix_data = {"time": ["2018-02-08T18:27:28.262Z", "2018-01-08T18:27:28.262Z"],
                           "matrix_row_name": ["1", "2"], "matrix_values": [[22,21], [23,24]], "matrix_columns": [["l1","l2"], ["l1","l2"]]}

    matrix_json = {
        "graphType": "MATRIX",
        "values": [
            {
                "name": "stat",
                "columns": [
                    "time",
                    "value"
                ],
                "values": [
                    [
                        "2018-02-08T18:27:28.262Z",
                        "{\"1\": {\"l1\": 22, \"l2\": 21}}"
                    ],
                    [
                        "2018-01-08T18:27:28.262Z",
                        "{\"2\": {\"l1\": 23, \"l2\": 24}}"
                    ]
                ]
            }
        ]
    }
    _check_data_frames(matrix_data, matrix_json, True)

    # MultiGraph
    multi_data = {"time": ["2018-02-08T18:27:28.262Z", "2018-01-08T18:27:28.262Z"],
                  "x_annotation": [[], []], "x_axis_tick_postfix": ["", ""],
                  "x_axis_type": ["Continuous", "Continuous"], "x_series": [[0,1,2,3], [0,1,2,3]],
                  "x_title": ["x_axis", "x_axis"], "y_title": ["y_axis", "y_axis"],
                  "y_annotation": [[], []],
                  "y_series": [[{"data": [0,1,2,3], "label":"yGraph1" }, {"data": [4,5,6,7],"label":"yGraph2"}],
                               [{"data": [10,11,12,13], "label":"yGraph1" }, {"data": [14,15,16,17],"label":"yGraph2"}]
                               ]}

    multi_json = {
        "graphType": "GENERAL_GRAPH",
        "values": [
            {
                "name": "stat",
                "columns": [
                    "time",
                    "value"
                ],
                "values": [
                    [
                        "2018-02-08T18:27:28.262Z",
                        "{\"x_title\": \"x_axis\", \"y_title\": \"y_axis\","
                        " \"x_axis_type\": \"Continuous\", \"x_axis_tick_postfix\": \"\","
                        " \"x_series\": [0, 1, 2, 3],"
                        " \"y_series\": [{\"data\": [0,1,2,3], \"label\": \"yGraph1\"},"
                        " {\"data\": [4,5,6,7], \"label\": \"yGraph2\"}],"
                        " \"x_annotation\": [], \"y_annotation\": []}"
                    ],
                    [
                        "2018-01-08T18:27:28.262Z",
                        "{\"x_title\": \"x_axis\", \"y_title\": \"y_axis\","
                        " \"x_axis_type\": \"Continuous\", \"x_axis_tick_postfix\": \"\","
                        " \"x_series\": [0, 1, 2, 3],"
                        " \"y_series\": [{\"data\": [10,11,12,13], \"label\": \"yGraph1\"},"
                        " {\"data\": [14,15,16,17], \"label\": \"yGraph2\"}],"
                        " \"x_annotation\": [], \"y_annotation\": []}"
                    ]
                ]
            }
        ]
    }
    _check_data_frames(multi_data, multi_json, True)


    # Negative tests
    _check_data_frames(linegraph_data, multilinegraph_json, False)

    with pytest.raises(MLOpsException):
        unexpected_json = {"key":"value"}
        data_frame_helper = DataFrameHelper()
        mlops_data_frame = data_frame_helper.create_data_frame(unexpected_json)


def test_empty_dataframe():
    dfh = DataFrameHelper()
    df1 = dfh.create_data_frame({})
    assert len(df1) == 0


def test_add_col_with_same_value():

    data = [[1, 2, 3], [22, 33, 44]]
    cols = ["A", "B", "C"]
    df1 = pd.DataFrame(data, columns=cols)

    # The test below will check that the method add_col_with_same_value will add a column called "agent" with a value
    # "localhost" to the df1 dataframe.
    col_name = "agent"
    value = "localhost"
    dfh = DataFrameHelper()
    dfh.add_col_with_same_value(df1, col_name, value)

    # Getting the "agents" column as a list
    agent_list = df1[col_name].tolist()
    print(agent_list)

    # Making sure that every item in the new column has the correct value
    for item in agent_list:
        assert item == value

def test_data_distribution_stats_to_dataframe():

    test_dict = [{
        'id': '51d20c34-775d-4537-af56-7c838a402dcf',
        'type': 'MLHealthModel',
        'data': '{"data":"{'
             '\\"c8\\":[{\\"-inf to 0.3668\\":0.023525},{\\"0.3668 to 0.49393\\":0.0334},{\\"0.49393 to 0.62106\\":0.058375},{\\"0.62106 to 0.74819\\":0.096375},{\\"0.74819 to 0.87532\\":0.13155},{\\"0.87532 to 1.00245\\":0.15635},{\\"1.00245 to 1.12958\\":0.155425},{\\"1.12958 to 1.25671\\":0.132425},{\\"1.25671 to 1.38384\\":0.09755},{\\"1.38384 to 1.51097\\":0.059625},{\\"1.51097 to 1.6381\\":0.0333},{\\"1.6381 to +inf\\":0.0221}],'
             '\\"c7\\":[{\\"-inf to 0.3693\\":0.022525},{\\"0.3693 to 0.49516000000000004\\":0.032475},{\\"0.49516000000000004 to 0.62102\\":0.059625},{\\"0.62102 to 0.74688\\":0.096375},{\\"0.74688 to 0.8727400000000001\\":0.1324},{\\"0.8727400000000001 to 0.9985999999999999\\":0.158425},{\\"0.9985999999999999 to 1.12446\\":0.154075},{\\"1.12446 to 1.25032\\":0.1317},{\\"1.25032 to 1.37618\\":0.097275},{\\"1.37618 to 1.50204\\":0.06065},{\\"1.50204 to 1.6279\\":0.031725},{\\"1.6279 to +inf\\":0.02275}],'
             '\\"c1\\":[{\\"-inf to 3.6389\\":0.008475},{\\"3.6389 to 5.90876\\":0.03405},{\\"5.90876 to 8.17862\\":0.09345},{\\"8.17862 to 10.44848\\":0.1459},{\\"10.44848 to 12.71834\\":0.129025},{\\"12.71834 to 14.988199999999999\\":0.072425},{\\"14.988199999999999 to 17.25806\\":0.07245},{\\"17.25806 to 19.52792\\":0.155375},{\\"19.52792 to 21.79778\\":0.179175},{\\"21.79778 to 24.06764\\":0.08875},{\\"24.06764 to 26.3375\\":0.019175},{\\"26.3375 to +inf\\":0.00175}],'
             '\\"c4\\":[{\\"-inf to 0.3674\\":0.022425},{\\"0.3674 to 0.49424999999999997\\":0.032975},{\\"0.49424999999999997 to 0.6211\\":0.059825},{\\"0.6211 to 0.7479499999999999\\":0.097375},{\\"0.7479499999999999 to 0.8748\\":0.13425},{\\"0.8748 to 1.00165\\":0.15525},{\\"1.00165 to 1.1284999999999998\\":0.1518},{\\"1.1284999999999998 to 1.25535\\":0.13205},{\\"1.25535 to 1.3821999999999999\\":0.097475},{\\"1.3821999999999999 to 1.5090499999999998\\":0.061675},{\\"1.5090499999999998 to 1.6359\\":0.033075},{\\"1.6359 to +inf\\":0.021825}],'
             '\\"c3\\":[{\\"-inf to 0.37\\":0.022525},{\\"0.37 to 0.49629\\":0.0324},{\\"0.49629 to 0.62258\\":0.058875},{\\"0.62258 to 0.74887\\":0.097375},{\\"0.74887 to 0.87516\\":0.1333},{\\"0.87516 to 1.0014500000000002\\":0.155875},{\\"1.0014500000000002 to 1.1277400000000002\\":0.15565},{\\"1.1277400000000002 to 1.2540300000000002\\":0.13455},{\\"1.2540300000000002 to 1.3803200000000002\\":0.09425},{\\"1.3803200000000002 to 1.5066100000000002\\":0.0609},{\\"1.5066100000000002 to 1.6329000000000002\\":0.031},{\\"1.6329000000000002 to +inf\\":0.0233}],'
             '\\"c6\\":[{\\"-inf to 0.3698\\":0.022475},{\\"0.3698 to 0.49567000000000005\\":0.032475},{\\"0.49567000000000005 to 0.62154\\":0.061525},{\\"0.62154 to 0.74741\\":0.09425},{\\"0.74741 to 0.8732800000000001\\":0.132925},{\\"0.8732800000000001 to 0.9991500000000001\\":0.154475},{\\"0.9991500000000001 to 1.1250200000000001\\":0.1594},{\\"1.1250200000000001 to 1.25089\\":0.130625},{\\"1.25089 to 1.37676\\":0.097875},{\\"1.37676 to 1.50263\\":0.05935},{\\"1.50263 to 1.6285000000000003\\":0.031075},{\\"1.6285000000000003 to +inf\\":0.023549999999999998}],'
             '\\"c9\\":[{\\"-inf to 0.3725\\":0.0228},{\\"0.3725 to 0.49798\\":0.0315},{\\"0.49798 to 0.6234599999999999\\":0.06125},{\\"0.6234599999999999 to 0.7489399999999999\\":0.096425},{\\"0.7489399999999999 to 0.87442\\":0.13315},{\\"0.87442 to 0.9999\\":0.15485},{\\"0.9999 to 1.1253799999999998\\":0.157125},{\\"1.1253799999999998 to 1.2508599999999999\\":0.132525},{\\"1.2508599999999999 to 1.37634\\":0.0928},{\\"1.37634 to 1.50182\\":0.062875},{\\"1.50182 to 1.6273\\":0.032225},{\\"1.6273 to +inf\\":0.022475}],'
             '\\"c0\\":[{\\"-inf to 16.1346\\":0.0124},{\\"16.1346 to 17.91226\\":0.03585},{\\"17.91226 to 19.68992\\":0.07990000000000001},{\\"19.68992 to 21.467579999999998\\":0.116325},{\\"21.467579999999998 to 23.24524\\":0.125925},{\\"23.24524 to 25.0229\\":0.12975},{\\"25.0229 to 26.80056\\":0.13205},{\\"26.80056 to 28.57822\\":0.12945},{\\"28.57822 to 30.35588\\":0.109025},{\\"30.35588 to 32.13354\\":0.076525},{\\"32.13354 to 33.9112\\":0.03855},{\\"33.9112 to +inf\\":0.01425}],'
             '\\"c2\\":[{\\"-inf to 3.6169\\":0.0087},{\\"3.6169 to 5.89061\\":0.033975},{\\"5.89061 to 8.16432\\":0.093925},{\\"8.16432 to 10.43803\\":0.144025},{\\"10.43803 to 12.711739999999999\\":0.129375},{\\"12.711739999999999 to 14.985449999999998\\":0.07445},{\\"14.985449999999998 to 17.25916\\":0.073075},{\\"17.25916 to 19.53287\\":0.1542},{\\"19.53287 to 21.80658\\":0.1785},{\\"21.80658 to 24.08029\\":0.089825},{\\"24.08029 to 26.354\\":0.01815},{\\"26.354 to +inf\\":0.0017999999999999997}],'
             '\\"c5\\":[{\\"-inf to 0.3682\\":0.02255},{\\"0.3682 to 0.49465000000000003\\":0.031925},{\\"0.49465000000000003 to 0.6211\\":0.059525},{\\"0.6211 to 0.74755\\":0.096675},{\\"0.74755 to 0.8740000000000001\\":0.1339},{\\"0.8740000000000001 to 1.00045\\":0.1547},{\\"1.00045 to 1.1269\\":0.156925},{\\"1.1269 to 1.2533500000000002\\":0.132125},{\\"1.2533500000000002 to 1.3798000000000001\\":0.096725},{\\"1.3798000000000001 to 1.50625\\":0.0597},{\\"1.50625 to 1.6327\\":0.032275},{\\"1.6327 to +inf\\":0.022975}]}",'
             '"graphType":"BARGRAPH",'
             '"timestamp":1522633474961999872,'
             '"mode":"INSTANT",'
             '"name":"continuousDataHistogram",'
             '"type":"Health"}'
      }]

    dfh = DataFrameHelper()

    for model_stat in test_dict:
        # TODO: check type and make sure it is ok
        data = model_stat['data']
        # Convery data to a dict
        decoded_data = ast.literal_eval(data)
        print("\n")
        print("graphType: {}".format(decoded_data["graphType"]))
        print("name:      {}".format(decoded_data["name"]))

        bargraph_data = decoded_data['data']
        bargraph_dict = ast.literal_eval(bargraph_data)

        hist_df = dfh.multi_histogram_df(bargraph_dict)
        print(hist_df)
        assert len(hist_df) == 10
        hist_type_list = hist_df["hist_type"].tolist()
        assert hist_type_list[0] == 'continuous'

        print(hist_type_list)


def test_histogram_stats(generate_da_with_missing_data):
    pd_data = pd.read_csv(generate_da_with_missing_data)

    features_values = np.array(pd_data.values)
    features_names = list(pd_data.columns)

    # generating general stats like categorical/continuous features and contender histograms.
    general_hist_stat = GeneralHistogramStat()
    general_hist_stat.max_cat_unique_values = 3
    general_hist_stat \
        .create_and_set_general_stat(set_of_features_values=features_values,
                                     set_of_features_names=features_names,
                                     model_stat=None)

    cat_features = ['Missing::All', 'Missing::Even', 'Missing::Seq', 'emptystrings']
    cont_features = ['Missing::Float']

    assert set(general_hist_stat.set_of_categorical_features) == set(cat_features)
    assert set(general_hist_stat.set_of_continuous_features) == set(cont_features)
