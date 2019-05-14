#!/usr/bin/env bash

script_name=$(basename ${BASH_SOURCE[0]})
script_dir=$(realpath $(dirname ${BASH_SOURCE[0]}))

USAGE="Usage: ${script_name} [--help] [--dir <artifacts-dir>] [--skip-train] [--skip-predict] [--verbose]"

function validate_arg {
    [ -z $2 ] && { echo "Missing '$1' arg value!"; exit -1; }
}

artifacts_dir=""
skip_train=0
skip_predict=0
verbose=0
while [ -n "$1" ]; do
    case $1 in
        --dir)          artifacts_dir=$2; validate_arg $1 $2 ; shift ;;
        --skip-train)   skip_train=1 ;;
        --skip-predict) skip_predict=1 ;;
        --verbose)      verbose=1 ;;
        --help)         echo $USAGE; exit 0 ;;
    esac
    shift
done

# General
artifacts_dir_prefix="/tmp/sagemaker-mnist-artifacts-"
if [ -z ${artifacts_dir} ]; then
    artifacts_dir=$(mktemp -d ${artifacts_dir_prefix}XXXXX)
else
    if [[ ${skip_train} == 0 ]]; then
        rm -rf ${artifacts_dir}
        mkdir -p ${artifacts_dir}
    fi
fi
echo "Artifacts dir: ${artifacts_dir}"

mlcomp_root="$script_dir/../.."
components_root="$mlcomp_root/tests/components/sagemaker"
model_filepath="${artifacts_dir}/sagemaker-kmeans-model.tar.gz"

if [[ ${skip_train} == 0 ]]; then
    echo
    echo ################################################################################
    echo # Training
    echo #
    deployment_path=${artifacts_dir}/train-mlpiper-deployment
    pipeline_filepath="$mlcomp_root/tests/pipelines/sagemaker_mnist_training.json"

    set -x
    PYTHONPATH=${mlcomp_root}:${mlcomp_root}/../mlops $mlcomp_root/bin/mlpiper run \
        --output-model ${model_filepath} \
        -f ${pipeline_filepath} \
        -r ${components_root} \
        -d ${deployment_path} \
        --force
    set +x

    if [ -f $model_filepath ]; then
        rm -rf $deployment_path
        echo
        echo "Training passed successfully! Model downloaded to: $model_filepath"
    else
        echo "Training failed!"
        exit -1
    fi
fi

if [[ ${skip_predict} == 0 ]]; then
    echo
    echo ################################################################################
    echo # Prediction
    echo #


    pipeline_comp_test_set_local_csv_filepath="/tmp/sagemaker_mnist_test_dataset.csv"
    pipeline_comp_attr_download_results="/tmp/sagemaker_mnist_test_dataset.out"

    deployment_path=${artifacts_dir}/predict-mlpiper-deployment
    pipeline_filepath="$mlcomp_root/tests/pipelines/sagemaker_mnist_prediction.json"
    prediction_results_filepath="${artifacts_dir}/sagemaker_mnist_test_dataset.out"

    set -x
    PYTHONPATH=${mlcomp_root}:${mlcomp_root}/../mlops ${mlcomp_root}/bin/mlpiper run \
        --input-model ${model_filepath} \
        -f ${pipeline_filepath} \
        -r ${components_root} \
        -d ${deployment_path} \
        --force
    set +x

    if [ -f ${pipeline_comp_attr_download_results} ]; then
        cp ${pipeline_comp_attr_download_results} ${prediction_results_filepath}
        rm -rf ${deployment_path}
        echo
        echo "Prediction passed successfully! Results are at: ${prediction_results_filepath}"
    else
        echo "Prediction failed!"
        exit -1
    fi
fi

[[ ${artifacts_dir} == "${artifacts_dir_prefix}*" ]] && { rm -rf ${artifacts_dir}; }

echo "Completed"
