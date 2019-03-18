#!/usr/bin/env bash
script_name=$(basename ${BASH_SOURCE[0]})
script_dir=$(realpath $(dirname ${BASH_SOURCE[0]}))

mlcomp_root="$script_dir/../.."
model_filepath="$mlcomp_root/tests/models/sklearn_kmeans_rest_model.pkl"
pipeline_filepath="$mlcomp_root/tests/pipelines/sklearn-rest-model-serving-pipeline.json"
components_root="$mlcomp_root/tests/components/RestModelServing/restful_sklearn_serving_test"
deployment_path=$(mktemp -d -t mlpiper-deployment-XXXXXXXXXX)

set -x
PYTHONPATH=$mlcomp_root:$mlcomp_root/../mlops $mlcomp_root/bin/mlpiper run \
    --input-model $model_filepath \
    -f $pipeline_filepath \
    -r $components_root \
    -d $deployment_path \
    --force
set +x

# rm -rf $deployment_path