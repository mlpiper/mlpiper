#!/usr/bin/env bash

SCRIPT_NAME=$(basename ${BASH_SOURCE[0]})
SCRIPT_DIR=$(realpath $(dirname ${BASH_SOURCE[0]}))

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
echo ==========================================================
$SCRIPT_DIR/mlpiper run-deployment --deployment-dir $SCRIPT_DIR
