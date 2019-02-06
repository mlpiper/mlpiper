#!/bin/bash

DEPLOY_DIR=/tmp/pp

rm -rf $DEPLOY_DIR

mlpiper --skip-mlpiper-deps \
	  -r ~/src/mlhub/components/ \
	  --logging-level error \
	   run -p ~/src/mlhub/pipelines/Generic/src-sink.json \
           --deployment-dir /tmp/pp
