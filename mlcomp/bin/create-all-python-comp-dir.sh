#!/usr/bin/env bash



component_dir=$(dirname $0)
component_dir=$component_dir/../../reflex-algos/components
component_dir=$(realpath $component_dir)

echo "Component Dir: $component_dir"
ls $component_dir

tmp_comp_dir=$1
echo "tmp_comp_dir: $tmp_comp_dir"

if [ -d $tmp_comp_dir ] ; then
    echo "Directory $tmp_comp_dir already exsists"
    exit 1
else
    mkdir $tmp_comp_dir
fi

echo "Copying python components: $component_dir/Python"
cd $component_dir
cp -a Python/* $tmp_comp_dir

echo "Generating and copying Java components"
cd $component_dir/Java
dirs=($(find . -type d -maxdepth 1  -name "test-*"))
echo "Dirs:"
echo "$dirs"
echo "-----"
for dir in "${dirs[@]}"; do
    component_name=$(basename $dir)
    echo "==== $dir - $component_name ===="
    cd $component_dir/Java
    cd "$dir"
    pwd
    echo "Building"
    mvn package
    cp -a target/$component_name $tmp_comp_dir/
    touch $tmp_comp_dir/$component_name/__init__.py
done

echo "---- DIR ----"
ls $tmp_comp_dir
