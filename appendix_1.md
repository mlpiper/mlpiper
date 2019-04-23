# Appendix: Heatmap Calculation

Heatmap graphs show a coarse-grained view of the variation in feature
values across time. These values are normalized to the range `[0,1]` and
displayed using color coding, where 0 is represented by white and 1 is
represented by dark blue. 

## Batch Environment

In batch scenarios, each feature is normalized using rescaling. For each
feature value, we subtract the minimum for this value and divide by the
range of the feature values. The mean of the normalized feature becomes
HeatMap value for respective feature. This value is calculated
independently per batch. 

For example, consider a feature** **with values 1, 2, 0, 4, 5. The
minimum value is 0 and the range is 5, so the rescaled values are 0.2,
0.3, 0, 0.8, and 1. The mean of these rescaled values (0.48 in this
case) is the HeatMap value.

## Streaming Environment

In streaming scenarios, an aggregate statistic of a feature is
calculated over local, disjoint windows of size N1 (currently set to
10,000). The mean value from each local window is then input into a
global window of size N2 (currently set to 30). This value is normalized
and the aggregate statistic calculated currently is the mean value of
the feature over the global window. Normalization across the global
window is done using the standardization method (from each feature
value, subtract with the mean of global window and divide with its
standard deviation value).
