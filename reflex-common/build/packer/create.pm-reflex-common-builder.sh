#!/usr/bin/env bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ $# -eq 0 ]; then
    TAG="latest"
else
    TAG="$1"
fi

sed "s/@@TAG@@/$TAG/" $SCRIPT_DIR/pm-reflex-common-builder.json.template > pm-reflex-common-builder.json

packer build pm-reflex-common-builder.json

# Remove json once build is complete
rm pm-reflex-common-builder.json
