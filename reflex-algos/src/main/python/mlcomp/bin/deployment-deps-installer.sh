#!/usr/bin/env bash

echo "Installing MLPiper required python packages..."
# TODO move to be in a requirement file
pip install kazoo
pip install protobuf
pip install requests
pip install py4j

