#!/usr/bin/env bash

MLPIPER_SKIP_DEPS_INSTALL=${MLPIPER_SKIP_DEPS_INSTALL:-0}

echo
# TODO: remove this part
if [ $MLPIPER_SKIP_DEPS_INSTALL -eq "0" ]
then

    echo "Installing MLPiper required python packages"
    # TODO move to be in a requirement file
    pip install kazoo
    pip install protobuf
    pip install requests
    pip install py4j
else
    echo "Skipping requirements installation"
fi
echo
echo
export PYTHONPATH=$PYTHONPATH:./

echo "Detecting all Python eggs in current directory"
for egg in *.egg
    do
        egg_full_path=$(realpath $egg)
        echo "Detected egg: $egg_full_path"
        PYTHONPATH=$PYTHONPATH:$egg_full_path
done


echo PYTHONPATH: $PYTHONPATH
echo "======= All Environment variables =========="
export
echo "============================================"

echo
echo
./mlpiper run-deployment --deployment-dir .