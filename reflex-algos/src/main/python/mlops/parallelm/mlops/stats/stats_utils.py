import six

from parallelm.mlops.mlops_exception import MLOpsException
from parallelm.mlops.stats.health.histogram_data_objects import CategoricalHistogramDataObject, \
    ContinuousHistogramDataObject


def check_list_of_str(list_of_str, error_prefix="Columns names"):
    if not isinstance(list_of_str, list):
        raise MLOpsException("{} should be provided as a list".format(error_prefix))

    if not all(isinstance(item, six.string_types) for item in list_of_str):
        raise MLOpsException("{} should be strings".format(error_prefix))


def check_vec_of_numbers(vec_of_numbers, error_prefix="Data"):
    if not isinstance(vec_of_numbers, list):
        raise MLOpsException("{} should be a list got {}".format(error_prefix, type(vec_of_numbers)))
    if not all(isinstance(item, (six.integer_types, float)) for item in vec_of_numbers):
        raise MLOpsException("{} should be a list of int or floats".format(error_prefix))


def check_hist_compare_requirement(inferring_hist_rep, contender_hist_rep, error_prefix="Hist Data"):
    if not (isinstance(inferring_hist_rep, list)):
        raise Exception("{} cannot be compared as inferring_hist_rep is not list type".format(error_prefix))

    if not isinstance(contender_hist_rep, list):
        raise Exception("{} cannot be compared as contender_hist_rep is not list type".format(error_prefix))

    if not (isinstance(inferring_hist_rep[0], CategoricalHistogramDataObject) or isinstance(inferring_hist_rep[0],
                                                                                            ContinuousHistogramDataObject)):
        raise Exception(
            "{} cannot be compared as inferring_hist_rep is CategoricalHistogramDataObject or ContinuousHistogramDataObject list type".format(
                error_prefix))

    if not (isinstance(contender_hist_rep[0], CategoricalHistogramDataObject) or isinstance(contender_hist_rep[0],
                                                                                            ContinuousHistogramDataObject)):
        raise Exception(
            "{} cannot be compared as contender_hist_rep is CategoricalHistogramDataObject or ContinuousHistogramDataObject list type".format(
                error_prefix))

    if not (type(contender_hist_rep[0]) == type(inferring_hist_rep[0])):
        raise Exception(
            "{} cannot be compared as type(contender_hist_rep): {} and type(inferring_hist_rep): {}".format(
                error_prefix, type(contender_hist_rep[0]), type(inferring_hist_rep[0])))

    if not all(isinstance(i_hist_rep, type(inferring_hist_rep[0])) for i_hist_rep in inferring_hist_rep):
        raise MLOpsException("{} in inferring hist representation list, all elements are not of same type".format(error_prefix))

    if not all(isinstance(c_hist_rep, type(contender_hist_rep[0])) for c_hist_rep in contender_hist_rep):
        raise MLOpsException("{} in contender hist representation list, all elements are not of same type".format(error_prefix))
