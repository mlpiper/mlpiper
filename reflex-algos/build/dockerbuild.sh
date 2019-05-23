#!/usr/bin/env bash

REFLEX_ALGO_DIR=`git rev-parse --show-toplevel`/reflex-algos
DOCKER_CONTAINER="parallelm/pm-mcenter-builder:1.0.0"

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
    -e "REPO_DIR=${REFLEX_ALGO_DIR}" \
    -v ${HOME}:${HOME} \
    -v ${HOME}/.m2:/tmp/m2 \
    ${DOCKER_CONTAINER} \
    ${REFLEX_ALGO_DIR}/build/docker-entrypoint.sh "$@"
