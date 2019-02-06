#!/usr/bin/env bash -e

script_name=$(basename ${BASH_SOURCE[0]})
script_dir=$(realpath $(dirname ${BASH_SOURCE[0]}))

# Python components
$script_dir/Python/restful_h2o_serving_source/create_component.sh
