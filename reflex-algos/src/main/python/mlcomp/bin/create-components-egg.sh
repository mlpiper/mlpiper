#!/usr/bin/env bash

script_name=$(basename ${BASH_SOURCE[0]})
script_dir=$(realpath $(dirname ${BASH_SOURCE[0]}))
all_components_dir_postfix="parallelm/code_components"

USAGE="Usage: ${script_name} [--help] [-c|--components-dir DIR] [--dst-dir DIR]"

components_dir="/non-existing-directory"
destination_dir="/tmp"

while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
    -c|--components-dir)
        components_dir="$2"
        shift
        ;;
    --dst-dir)
        destination_dir="$2"
        shift
        ;;
    -h|--help)
        echo $USAGE
        exit 0
        ;;
    *)
        echo $USAGE
        exit 1
        ;;
    esac
    shift # past argument or value
done


function create_components_egg {
    component_root_dir=$1
    dest_dir=$2

    tmp_dir=$(mktemp -d  /tmp/mlcomp_comp_egg_prep_XXXXXXXX)
    dst_comp_tmp_dir=$tmp_dir/$all_components_dir_postfix
    mkdir -p $dst_comp_tmp_dir

    # Copy everything to the destination path
    cp -r $component_root_dir/* $dst_comp_tmp_dir

    cp $script_dir/mcenter_components_setup.py $tmp_dir/setup.py

    touch $dst_comp_tmp_dir/__init__.py
    echo "__import__('pkg_resources').declare_namespace(__name__)" > $dst_comp_tmp_dir/../__init__.py

    ${script_dir}/create-egg.sh --root=${tmp_dir}  # --silent

    cp ${tmp_dir}/dist/*.egg ${dest_dir}

    rm -rf $tmp_dir
}


echo "Components dir:   $components_dir"
echo "Destination dir:  $destination_dir"
create_components_egg $components_dir $destination_dir
