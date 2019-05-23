#!/usr/bin/env bash

REFLEX_COMMON_DIR=`git rev-parse --show-toplevel`/reflex-common
DOCKER_CONTAINER="parallelm/pm-mcenter-builder:1.0.0"

echo "REFLEX_COMMON_DIR : $REFLEX_COMMON_DIR"

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
    -e "REPO_DIR=${REFLEX_COMMON_DIR}" \
    -v ${HOME}:${HOME} \
    -v ${HOME}/.m2:/tmp/m2 \
    ${DOCKER_CONTAINER} \
    ${REFLEX_COMMON_DIR}/build/docker-entrypoint.sh "$@"
