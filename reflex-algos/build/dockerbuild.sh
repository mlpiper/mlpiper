#!/usr/bin/env bash

Reflex_Algo_DIR=`git rev-parse --show-toplevel`/reflex-algos

if [ ! -d ${HOME}/.m2 ]; then
    mkdir ${HOME}/.m2
fi

docker run \
    --rm \
    -e "HOME=$HOME" \
    -e "HOST_UID=`id -u`" \
    -e "HOST_GID=`id -g`" \
    -e "HOST_USERNAME=`id -un`" \
    -e "HOST_GROUPNAME=`id -gn`" \
    -e "REPO_DIR=${Reflex_Algo_DIR}" \
    -v ${HOME}:${HOME} \
    -v ${HOME}/.m2:/tmp/m2 \
    parallelm/pm-reflex-algo-builder:latest \
    ${Reflex_Algo_DIR}/build/docker-entrypoint.sh "$@"
