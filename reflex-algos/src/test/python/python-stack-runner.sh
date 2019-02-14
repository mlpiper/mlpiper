#!/bin/bash

# TODO: FIX THIS HELP
#
# This script is performing the following steps as preparation for running a Runner test
#
# o Generate the mlops, mlcomp and deputy eggs
# o Copy the python components directory and pack all components as tarballs
#
# Arguments to script
# $1 pipeline file to use
# $2 (optional) top reflex directory, needed in order to access the sub/reflex-algos ...
#
#
#  TODO: in the future add the engine to test as an argument, and use this for testing other runners as well

echo "Python stack runner"

script_dir=$(dirname $0)
script_dir=$(realpath $script_dir)

pipeline_file_path=$1
reflex_top=${2:-$script_dir/../../../../../}

echo "Pipeline file: $pipeline_file_path"
echo "Reflex top:    $reflex_top"

pipeline_file_path=$(realpath $pipeline_file_path)
pipeline_file=$(basename $pipeline_file_path)
component_source_dir=$reflex_top/sub/reflex-algos/components/Python

python_dir=$reflex_top/sub/reflex-algos/src/main/python

mlcomp_dir=$python_dir/mlcomp
mlops_dir=$python_dir/mlops
deputy_dir=$python_dir/deputy

python="/usr/local/bin/python2"

deputy_egg_path="dist/deputy-1.0-py2.7.egg"
deputy_egg=$(basename $deputy_egg_path)

mlcomp_egg_path="dist/mlcomp-1.0.2-py2.7.egg"
mlops_egg_path="dist/parallelm-1.0.1-py2.7.egg"
comp_egg="mcenter_components-0.1-py2.7.egg"

echo "Script dir:       $script_dir"
echo "PWD:              $PWD"
echo "Reflex top dir:   $1"
echo "Pipeline file:    $2"
echo "Component source: $component_source_dir"
echo "python_dir:       $python_dir"


artifact_dir=$(mktemp -d /tmp/dd.XXXXX)

function generate_eggs() {
    echo "Generating eggs"

    echo "Making mlcomp egg: $mlcomp_dir"
    cd $mlcomp_dir
    pwd
    ls
    make egg
    cp $mlcomp_egg_path $artifact_dir
    echo

    echo "Making mlops egg"
    cd $mlops_dir
    make egg
    cp $mlops_egg_path $artifact_dir
    echo

    echo "Making deputy egg"
    cd $deputy_dir
    make egg
    cp $deputy_egg_path $artifact_dir
    echo

    echo "Making components egg"
    cd $mlcomp_dir
    ./bin/create-components-egg.sh --components-dir $component_source_dir --dst-dir $artifact_dir
    echo

    echo "----------"
    ls -la $artifact_dir
    echo "---------"
    echo
}

function gen_pythonpath() {
    PYTHONPATH=$deputy_dir
    PYTHONPATH=$PYTHONPATH:$artifact_dir/`basename $mlcomp_egg_path`
    PYTHONPATH=$PYTHONPATH:$artifact_dir/`basename $mlops_egg_path`
    PYTHONPATH=$PYTHONPATH:$artifact_dir/$comp_egg
    PYTHONPATH=$PYTHONPATH:$artifact_dir/`basename $deputy_egg_path`

    echo $PYTHONPATH
}


# MAIN
echo "========= main ========="
echo "Artifact dir: $artifact_dir"

generate_eggs

echo "Running the pipeline file"
cd $artifact_dir
ls -la

echo "Running -------"
echo "PYTHONPATH: " gen_pythonpath

env PYTHONPATH=$(gen_pythonpath) python $deputy_egg --pipeline-file $pipeline_file_path --logging-level debug

echo "Removing $artifact_dir"
rm -rf $artifact_dir
echo "Done"
