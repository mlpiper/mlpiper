#!/usr/bin/env bash

script_name=$(basename ${BASH_SOURCE[0]})
script_dir=$(realpath $(dirname ${BASH_SOURCE[0]}))

root_dir="${1:-${script_dir}/..}"
[ -d ${root_dir} ] || { echo "Root dir [${root_dir}] does not exist! Usage: ${script_name} <root-dir>"; exit -1; }

rm -rf ${root_dir}/build ${root_dir}/dist ${root_dir}/*.egg-info

find ${root_dir} -name "*.pyc" -exec rm {} \;

