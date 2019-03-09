#!/usr/bin/env bash

script_name=$(basename ${BASH_SOURCE[0]})
script_dir=$(realpath $(dirname ${BASH_SOURCE[0]}))


USAGE="Usage: ${script_name} [--root=<path>] [--silent] [--help]"

silent=""
root_dir="${script_dir}/.."
for p in "$@" ; do
    case $p in
        --root=*)       root_dir="${p#--root=}" ;;
        --silent)       silent="-q" ;;
        --help)         echo $USAGE; exit 0 ;;
        *)                  echo "Invalid argument!"; echo $USAGE; exit 0 ;;
    esac
done


${script_dir}/cleanup.sh ${root_dir}

cd ${root_dir}
python2 setup.py ${silent} bdist_egg
python3 setup.py ${silent} bdist_egg

tmp_path=$(mktemp -d)
mv dist ${tmp_path}
${script_dir}/cleanup.sh ${root_dir}
mv ${tmp_path}/dist ${root_dir}
rm -rf ${tmp_path}
