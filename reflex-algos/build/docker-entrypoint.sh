#!/bin/sh

# Add user and group inside container that matches the host
groupadd --gid $HOST_GID $HOST_GROUPNAME
adduser --uid $HOST_UID --gid $HOST_GID --home $HOME --no-create-home $HOST_USERNAME

# Use the ~/.m2 directories from the host system
ln -s /tmp/m2 ${HOME}/.m2

# Run the build
DEFAULT_BUILD="mvn clean install -Dmaven.javadoc.skip=true"
MVN_ARGS=""

if [ ! -z "$1" ] && [ "$1" == "-DskipTests" ]; then
    MVN_ARGS="-DskipTests"
fi

if [ $# -eq 0 ] || [ ! -z $MVN_ARGS ]; then
    echo -n "Building Reflex Algo ..."
    sudo -E -u $HOST_USERNAME /bin/sh -c "cd ${REPO_DIR} && $DEFAULT_BUILD $MVN_ARGS"
    last_cmd_status="$?"
    if [ $last_cmd_status -eq 0 ]; then
        echo "SUCCESS!!"
    else
        echo "FAILED!!!!"
        echo "Return code : $last_cmd_status"
    fi
    exit $last_cmd_status
else
    echo "Running provided script $@"
    cmd_to_run="$@"
    sudo -E -u $HOST_USERNAME /bin/sh -c "cd ${REPO_DIR} && $cmd_to_run"
fi
