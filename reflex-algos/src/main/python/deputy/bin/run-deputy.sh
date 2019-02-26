#!/usr/bin/env bash


echo "Running deputy"
script_dir=$(dirname $0)
echo "Script dir: $script_dir"


python="/usr/local/bin/python2"
py_major_ver=$($python -c "import sys; print(sys.version_info.major)")

deputy_egg_path="dist/deputy-1.0-py2.7.egg"
pipeline_file_path="bin/test-pipeline.json"
mlcomp_egg_path="../mlcomp/dist/ml_comp-*-py2.7.egg"
mlops_egg_path="../mlops/dist/parallelm-1.0.1-py2.7.egg"
comp_egg_path="/tmp/python_mcenter_components-0.1-py2.7.egg"

deputy_egg=$(basename $deputy_egg_path)
pipeline_file=$(basename $pipeline_file_path)


container_name="mcenter-container"

deputy_dir=$(mktemp -d /tmp/dd.XXXXX)

cp $deputy_egg_path $deputy_dir/
cp $mlcomp_egg_path $deputy_dir/
cp $mlops_egg_path $deputy_dir/
cp $comp_egg_path $deputy_dir/
cp $pipeline_file_path $deputy_dir/
echo "----------"
ls -la $deputy_dir

echo "---------"
cd $deputy_dir

export PYTHONPATH=$deputy_dir
export PYTHONPATH=$PYTHONPATH:$deputy_dir/`basename $mlcomp_egg_path`
export PYTHONPATH=$PYTHONPATH:$deputy_dir/`basename $mlops_egg_path`
export PYTHONPATH=$PYTHONPATH:$deputy_dir/`basename $comp_egg_path`
export PYTHONPATH=$PYTHONPATH:$deputy_dir/`basename $deputy_egg_path`

echo "---- PYTHONPATH -----"
echo $PYTHONPATH
echo "----------"


docker cp $deputy_dir $container_name:/tmp


docker exec --workdir $deputy_dir \
            -e PYTHONPATH=$PYTHONPATH \
            -it $container_name  \
            python $deputy_egg --pipeline-file $pipeline_file --logging-level debug


rm -rf $deputy_dir
echo "Done"
