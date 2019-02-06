from __future__ import print_function

import numpy as np
import pandas as pd
import scipy.stats
from parallelm.mlops import StatCategory as st
from parallelm.mlops import mlops as pm
from parallelm.mlops.examples import utils
from parallelm.mlops.examples.utils import RunModes
from parallelm.mlops.stats.graph import MultiGraph
from parallelm.mlops.stats.multi_line_graph import MultiLineGraph
from parallelm.mlops.stats.table import Table

"""
This code calculates the A/B test statistics.
- Points for two distributions
- Vertical line values on x-axis
- Conversion rate control
- Conversion rate B
- Relative uplift in conversion rate
- p-value
- z-score
- Standard error A
- Standard error B
- Standard error of difference
"""


class statsCalculator:
    def __init__(self):
        self._distControl = []
        self._distB = []
        self._conversionControl = []
        self._conversionB = []
        self._uplift = []
        self._pValue = []
        self._zScore = []
        self._errorControl = []
        self._errorB = []
        self._errorDifference = []
        self._verticalLine = []

    """
    This function calculates all the statistics for A/B testing after the experiment is complete
    Input: samples_control: Number of samples in control group
           conv_samples: Number of samples converted (could be from any metric),
            strictly less than samples_control
           samples_B: Number of samples in group B
           conv_samples_B: Number of samples converted (could be from any metric),
            strictly less than samples_B
    """

    def exptOutcome(self, samples_control, conv_samples_control, samples_B, conv_samples_B,
                    confidence):
        self._conversionControl = conv_samples_control * 100.0 / samples_control
        self._conversionB = conv_samples_B * 100.0 / samples_B

        if self._conversionControl != 0.0:
            self._uplift = (self._conversionB - self._conversionControl) * 100.0 /\
                           self._conversionControl
        else:
            self._uplift = self._conversionB * 100.0

        self._errorControl = np.sqrt(self._conversionControl / 100.0 * (
        1 - (self._conversionControl / 100.0)) / samples_control)
        self._errorB = np.sqrt(
            self._conversionB / 100.0 * (1 - (self._conversionB / 100.0)) / samples_B)
        self._errorDifference = np.sqrt(self._errorControl ** 2 + self._errorB ** 2)
        self._zScore = (self._conversionB - self._conversionControl) / (100.0 * self._errorDifference)
        if np.sign(self._zScore) == -1:
            self._pValue = 1 - scipy.stats.norm.sf(abs(self._zScore))
        else:
            self._pValue = scipy.stats.norm.sf(abs(self._zScore))
        self._distControl = self.calDist(samples_control, conv_samples_control)
        self._distB = self.calDist(samples_B, conv_samples_B)

        sigma = np.sqrt(samples_control * self._conversionControl / 100.0 * (
        1 - self._conversionControl / 100.0))
        if (confidence == 90):
            self._verticalLine = conv_samples_control + 1.645 * sigma
        elif (confidence == 95):
            self._verticalLine = conv_samples_control + 1.96 * sigma
        elif (confidence == 99):
            self._verticalLine = conv_samples_control + 2.575 * sigma
        else:
            raise ValueError("confidence value should either be 90, 95 or 99")

    """
    This function calculated the (x,y) values for a line plot of the binomial distribution
    Input: samples: Number of samples in the binomial experiment
           conv_samples: Number of samples converted (could be from any metric), strictly less than conv_samples
    return: distribution_x_axis: X axis points where probability mass is calculated
            distribution_y_axis: Y axis points which represents the probability mass
    """

    def calDist(self, samples, conv_samples):
        probability = conv_samples / samples
        sigma = np.sqrt(samples * probability * (1 - probability))
        # Lets capture 3sigma variation with n=20 points to plot
        n = 21
        distribution_points = 8 * sigma / n
        distribution_x_axis = (conv_samples - 4 * sigma + 8 * sigma / n) + distribution_points * \
                                                                           np.arange(n)
        distribution_x_axis = distribution_x_axis.astype(int)
        distribution_y_axis = scipy.stats.binom.pmf(distribution_x_axis, samples, probability)
        return distribution_x_axis, distribution_y_axis

    """
    This function returns the confidence
    """

    def calConfidence(self):
        return (1 - self._pValue) * 100

    """
    This function returns true if the confidence is more than the given value, false otherwise.
    This is same as the top banner you see on the screen in: https://abtestguide.com/calc/
    Input: value: Confidence value usually one of these three values: 90, 95, 99
    Output: True or False
    """

    def calSuccess(self, value):
        if self.calConfidence() > value and self._uplift > 0:
            return True
        else:
            return False


def ab_test(options, start_time, end_time, mode):
    sc = None
    if mode == RunModes.PYSPARK:
        from pyspark import SparkContext
        sc = SparkContext(appName="pm-ab-testing")
        pm.init(sc)
    elif mode == RunModes.PYTHON:
        pm.init()
    else:
        raise Exception("Invalid mode " + mode)

    not_enough_data = False

    # Following are a and b component names
    a_prediction_component_name = options.nodeA
    b_prediction_component_name = options.nodeB

    conv_a_stat_name = options.conversionsA
    conv_b_stat_name = options.conversionsB

    samples_a_stat_name = options.samplesA
    samples_b_stat_name = options.samplesB

    a_agent = utils._get_agent_id(a_prediction_component_name, options.agentA)
    b_agent = utils._get_agent_id(b_prediction_component_name, options.agentB)

    if a_agent is None or b_agent is None:
        print("Invalid agent provided {} or {}".format(options.agentA, options.agentB))
        pm.system_alert("PyException",
                        "Invalid Agent {} or {}".format(options.agentA, options.agentB))
        return

    try:
        a_samples = pm.get_stats(name=samples_a_stat_name, mlapp_node=a_prediction_component_name,
                                 agent=a_agent, start_time=start_time,
                                 end_time=end_time)

        b_samples = pm.get_stats(name=samples_b_stat_name, mlapp_node=b_prediction_component_name,
                                 agent=b_agent, start_time=start_time,
                                 end_time=end_time)

        a_samples_pdf = pd.DataFrame(a_samples)
        b_samples_pdf = pd.DataFrame(b_samples)

        try:
            rowa1 = int(a_samples_pdf.tail(1)['value'])
            rowb1 = int(b_samples_pdf.tail(1)['value'])
        except Exception as e:
            not_enough_data = True
            print("Not enough samples stats produced in pipelines")
            raise ValueError("Not enough data to compare")

        a_conv = pm.get_stats(name=conv_a_stat_name, mlapp_node=a_prediction_component_name,
                              agent=a_agent, start_time=start_time,
                              end_time=end_time)
        b_conv = pm.get_stats(name=conv_b_stat_name, mlapp_node=b_prediction_component_name,
                              agent=b_agent, start_time=start_time,
                              end_time=end_time)

        a_conv_pdf = pd.DataFrame(a_conv)
        b_conv_pdf = pd.DataFrame(b_conv)

        try:
            rowa2 = int(a_conv_pdf.tail(1)['value'])
            rowb2 = int(b_conv_pdf.tail(1)['value'])
        except Exception as e:
            not_enough_data = True
            print("Not enough conversion stats produced in pipelines")
            raise ValueError("Not enough data to compare")

        abHealth = statsCalculator()
        abHealth.exptOutcome(float(rowa1), float(rowa2), float(rowb1), float(rowb2),
                             options.confidence)
        confidence = abHealth.calConfidence()
        out = abHealth.calSuccess(options.confidence)

        # calculate conversion rate
        convA = float(rowa2) / float(rowa1)
        convB = float(rowb2) / float(rowb1)
        if convA != 0.0:
            relUplift = (convB - convA) / (convA)
        else:
            relUplift = convB
        relUplift = relUplift * 100

        # AB Graphs
        ab = MultiGraph().name("AB").set_continuous()

        ab.x_title("Conversion Rate (%)")
        ab.y_title(" ")

        # normalizing x and y axis for A for display
        dist_a_norm_x = [a_x * 100.0 / rowa1 for a_x in abHealth._distControl[0].tolist()]
        dist_a_norm_y = [a_y * rowa1 / 100.0 for a_y in abHealth._distControl[1].tolist()]
        ab.add_series(label="A", x=dist_a_norm_x, y=dist_a_norm_y)

        # normalizing x and y axis for B for display
        dist_b_norm_x = [b_x * 100.0 / rowb1 for b_x in abHealth._distB[0].tolist()]
        dist_b_norm_y = [b_y * rowb1 / 100.0 for b_y in abHealth._distB[1].tolist()]
        ab.add_series(label="B", x=dist_b_norm_x, y=dist_b_norm_y)

        # annotate confidence line on normalized x-axis
        ab.annotate(label="{} %".format(options.confidence),
                    x=abHealth._verticalLine * 100.0 / rowa1)

        # for not overriding it in display
        # annotate CR line on normalized x-axis
        if convA != convB:
            ab.annotate(label="CR A {}".format(convA * 100.0), x=convA * 100.0)
            ab.annotate(label="CR B {}".format(convB * 100.0), x=convB * 100.0)
        else:
            ab.annotate(label="CR A & B {}".format(convA * 100.0), x=convA * 100.0)

        pm.set_stat(ab)

        # conversion rate
        cols = ["A", "B"]
        mlt = MultiLineGraph().name("ConversionRate").labels(cols).data(
            [convA * 100.0, convB * 100.0])
        pm.set_stat(mlt)

        # emit table with all stats
        tbl2 = Table().name("AB Stats").cols(
            ["Samples Processed", "Conversions", "Conversion Rate (%)",
             "Improvement (%)", "Chance to beat baseline (%)"])
        tbl2.add_row(options.champion,
                     [str(rowa1), str(rowa2), "{0:.2f}".format(convA * 100), "-", "-"])
        tbl2.add_row(options.challenger, [str(rowb1), str(rowb2), "{0:.2f}".format(convB * 100),
                                          "{0:.2f}".format(relUplift),
                                          "{0:.2f}".format(confidence)])
        pm.set_stat(tbl2)

        # set cookie
        tbl = Table().name("cookie").cols(["uplift", "champion", "challenger",
                                           "conversionA", "conversionB", "realUplift", "success",
                                           "confidence", "realConfidence"])
        tbl.add_row("1", [str(options.uplift), options.champion, options.challenger,
                          "{0:.2f}".format(convA * 100), "{0:.2f}".format(convB * 100),
                          "{0:.2f}".format(abHealth._uplift), str(out), str(options.confidence),
                          "{0:.2f}".format(abHealth.calConfidence())])
        pm.set_stat(tbl)

        if out == True:
            pm.data_alert("DataAlert", "AB Test Success zScore {}".format(abHealth._zScore))
            pm.set_stat("Success", 1, st.TIME_SERIES)
        else:
            pm.set_stat("Success", 0, st.TIME_SERIES)

    except Exception as e:
        if not_enough_data is False:
            print("Got exception while getting stats: {}".format(e))
            pm.system_alert("PyException", "Got exception {}".format(e))

    if mode == RunModes.PYSPARK:
        sc.stop()
    pm.done()


'''
import abStats
samples_control = 80000
samples_B = 80000
conv_control = 1600
conv_B = 1700
abHealth = abStats.statsCalculator()
abHealth.exptOutcome(samples_control, conv_control, samples_B, conv_B)
out_90 = abHealth.calConfidence(90)
out_95 = abHealth.calConfidence(95)
out_99 = abHealth.calConfidence(99)
plt.plot(abHealth._distControl[0], abHealth._distControl[1], 'ro')
plt.plot(abHealth._distB[0], abHealth._distB[1], 'ro')
plt.show()
'''
