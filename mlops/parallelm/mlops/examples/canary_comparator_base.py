from __future__ import print_function

import copy
import math
import traceback
from itertools import tee

import pandas as pd
from parallelm.mlops import StatCategory as st
from parallelm.mlops import mlops as mlops
from parallelm.mlops.events.canary_alert import CanaryAlert
from parallelm.mlops.stats.bar_graph import BarGraph
from parallelm.mlops.stats.graph import MultiGraph
from parallelm.mlops.examples.utils import RunModes
from parallelm.mlops.examples import utils


def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def _compare_cat_hist(b1, b2, h1, h2):
    """
    Compare two categorical histograms and return a overlap score based on RMSE
    b1 bin edges of hist 1
    b2 bin edges of hist 2
    h1 histogram values of hist 1
    h2 histogram values of hist 2

    Return rmse-based overlap score
    """
    cbe = list(set(b1) | set(b2))

    total = len(cbe)
    rmse = 0.0

    if sum(h1) == 0 or sum(h2) == 0:
        return 0.0

    for index in range(total):
        sh1 = 0.0
        sh2 = 0.0
        try:
            sh1 = float(h1[b1.index(cbe[index])])
        except Exception as e:
            sh1 = 0.0
        try:
            sh2 = float(h2[b2.index(cbe[index])])
        except Exception as e:
            sh2 = 0.0

        sh1 = sh1 / sum(h1)
        sh2 = sh2 / sum(h2)
        rmse += ((sh1 - sh2) ** 2)

    rmse = (rmse) ** 0.5
    print("Cat: rmse score: {}".format(rmse))
    return rmse


def _compare_cont_hist(b1, b2, h1, h2):
    """
    Compare two continuous histograms and return a overlap score based on RMSE
    b1 bin edges of hist 1
    b2 bin edges of hist 2
    h1 histogram values of hist 1
    h2 histogram values of hist 2

    Return rmse-based overlap score
    """

    b1 = copy.deepcopy(b1)
    h1 = copy.deepcopy(h1)
    b2 = copy.deepcopy(b2)
    h2 = copy.deepcopy(h2)

    bd1 = [float(x) for x in b1]
    bd2 = [float(x) for x in b2]

    inf = float('inf')

    if bd1[0] == -inf:
        del bd1[0]
        del h1[0]
    if bd1[-1] == inf:
        del bd1[-1]
        del h1[-1]
    if bd2[0] == -inf:
        del bd2[0]
        del h2[0]
    if bd2[-1] == inf:
        del bd2[-1]
        del h2[-1]

    cbe = sorted(list(set(bd1) | set(bd2)))

    total = len(cbe)

    curr1 = 0
    curr2 = 0
    init = False
    rmse = 0.0

    if sum(h1) == 0 or sum(h2) == 0:
        return 0

    for index in range(total):
        if init is False:
            init = True
            prev1 = 0
            prev2 = 0
        else:
            if (curr1 > prev1 and curr1 < len(bd1)):
                sh1 = float(h1[prev1] * (cbe[index] - cbe[index - 1])) / (bd1[curr1] - bd1[prev1])
            else:
                sh1 = 0.0
            if (curr2 > prev2 and curr2 < len(bd2)):
                sh2 = float(h2[prev2] * (cbe[index] - cbe[index - 1])) / (bd2[curr2] - bd2[prev2])
            else:
                sh2 = 0.0

            if math.isnan(sh1) is False and math.isnan(sh2) is False:
                sh1 = sh1 / sum(h1)
                sh2 = sh2 / sum(h2)
                rmse += ((sh1 - sh2) ** 2)

        if (curr1 < len(bd1) and bd1[curr1] <= cbe[index]):
            prev1 = curr1
            curr1 += 1
        if (curr2 < len(bd2) and bd2[curr2] <= cbe[index]):
            prev2 = curr2
            curr2 += 1

    rmse = (rmse) ** 0.5

    print("Cont: rmse score: {}".format(rmse))
    return rmse


def canary_comparator(options, start_time, end_time, mode):
    sc = None
    if mode == RunModes.PYSPARK:
        from pyspark import SparkContext
        sc = SparkContext(appName="canary-comparator")
        mlops.init(sc)
    elif mode == RunModes.PYTHON:
        mlops.init()
    else:
        raise Exception("Invalid mode " + mode)

    not_enough_data = False

    # Following are main and canary component names
    main_prediction_component_name = options.nodeA
    canary_prediction_component_name = options.nodeB

    main_stat_name = options.predictionHistogramA
    canary_stat_name = options.predictionHistogramB

    main_agent = utils._get_agent_id(main_prediction_component_name, options.agentA)
    canary_agent = utils._get_agent_id(canary_prediction_component_name, options.agentB)
    if main_agent is None or canary_agent is None:
        print("Invalid agent provided {} or {}".format(options.agentA, options.agentB))
        mlops.system_alert("PyException",
                           "Invalid Agent {} or {}".format(options.agentA, options.agentB))
        return

    try:
        main_data_frame = mlops.get_stats(name=main_stat_name,
                                          mlapp_node=main_prediction_component_name,
                                          agent=main_agent,
                                          start_time=start_time,
                                          end_time=end_time)

        canary_data_frame = mlops.get_stats(name=canary_stat_name,
                                            mlapp_node=canary_prediction_component_name,
                                            agent=canary_agent,
                                            start_time=start_time,
                                            end_time=end_time)

        main_pdf = pd.DataFrame(main_data_frame)
        canary_pdf = pd.DataFrame(canary_data_frame)

        try:
            row1 = main_pdf.tail(1).iloc[0]
            row2 = canary_pdf.tail(1).iloc[0]
        except Exception as e:
            not_enough_data = True
            print("Not enough histograms produced in pipelines")
            raise ValueError("Not enough data to compare")

        if row1['hist_type'] != row2['hist_type']:
            raise ValueError('Canary and Main pipelines dont produce histograms'
                             + 'of same type {} != {}'.format(row1['hist_type'], row2['hist_type']))

        if row1['hist_type'] == 'continuous':
            rmse = _compare_cont_hist(row1['bin_edges'], row2['bin_edges'], row1['hist_values'],
                                      row2['hist_values'])
            gg2 = MultiGraph().name("Prediction Histograms").set_categorical()

            gg2.x_title("Predictions")
            gg2.y_title("Normalized Frequency")

            gg2.add_series(label="Main", x=[float(x) for x in row1['bin_edges']][:-1],
                           y=[y for y in row1['hist_values']])
            gg2.add_series(label="Canary", x=[float(x) for x in row2['bin_edges']][:-1],
                           y=[y for y in row2['hist_values']])
            mlops.set_stat(gg2)

            bar1 = BarGraph().name("Main Pipeline").cols(
                ["{} to {}".format(x, y) for (x, y) in pairwise(row1['bin_edges'])]).data(
                [x for x in row1['hist_values']])
            mlops.set_stat(bar1)

            bar2 = BarGraph().name("Canary Pipeline").cols(
                ["{} to {}".format(x, y) for (x, y) in pairwise(row2['bin_edges'])]).data(
                [x for x in row2['hist_values']])
            mlops.set_stat(bar2)

        elif row1['hist_type'] == 'categorical':
            rmse = _compare_cat_hist(row1['bin_edges'], row2['bin_edges'], row1['hist_values'],
                                     row2['hist_values'])

            gg2 = MultiGraph().name("Prediction Histograms").set_categorical()

            gg2.x_title("Predictions")
            gg2.y_title("Normalized Frequency")

            gg2.add_series(label="Main", x=row1['bin_edges'],
                           y=[y for y in row1['hist_values']])
            gg2.add_series(label="Canary", x=row2['bin_edges'],
                           y=[y for y in row2['hist_values']])
            mlops.set_stat(gg2)

            bar1 = BarGraph().name("Main Pipeline").cols(
                ["{}".format(x) for x in row1['bin_edges']]).data(
                [x for x in row1['hist_values']])
            mlops.set_stat(bar1)

            bar2 = BarGraph().name("Canary Pipeline").cols(
                ["{}".format(x) for x in row2['bin_edges']]).data(
                [x for x in row2['hist_values']])
            mlops.set_stat(bar2)
        else:
            raise ValueError('Invalid histogram type: {}'.format(row1['hist_type']))

        mlops.set_stat("RMSE", rmse, st.TIME_SERIES)

        print("mlops policy {}".format(mlops.mlapp_policy))

        if mlops.mlapp_policy.canary_threshold is None:
            print("Canary health threshold not set")
            raise ValueError("Canary health threshold not set in config")

        # Following code perform comparison between the histograms.
        # Here you can insert your own code
        if rmse > mlops.mlapp_policy.canary_threshold:
            print("Canary Alert {} > {}".format(rmse, mlops.mlapp_policy.canary_threshold))
            mlops.event(CanaryAlert(label="CanaryAlert", is_healthy=False, score=rmse,
                                    threshold=mlops.mlapp_policy.canary_threshold))
        else:
            print("Data matches {}".format(rmse))
            mlops.event(CanaryAlert(label="CanaryAlert", is_healthy=True, score=rmse,
                                    threshold=mlops.mlapp_policy.canary_threshold))

    except Exception as e:
        if not_enough_data is False:
            print("Got exception while getting stats: {}".format(e))
            mlops.system_alert("PyException", "Got exception {}".format(traceback.format_exc()))

    if mode == RunModes.PYSPARK:
        sc.stop()
    mlops.done()
