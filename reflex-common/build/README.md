# Reflex-Common Build

## Requirements

* OSX or Linux system (currently do not support Windows) * [Docker](https://docs.docker.com/engine/installation/) installation
* Reflex-common Build container
([Guide](https://bitbucket.org/parallelmachines/reflex-common/build/packer/README.md))

## Containerized User Builds
The `dockerbuild.sh` script can be used to run a build inside of the official build environments
produced by [Packer](https://bitbucket.org/parallelmachines/reflex-common/build/packer/README.md).

**NOTE:** Containers are run in
*[ephemeral](https://docs.docker.com/engine/reference/run/#/clean-up---rm)*
mode which means there is no saved state between runs. Any changes done outside of the shared
directories will be lost.

### How it works
In order to build your local source tree inside the container the following things are done by
the build scripts.

* Figure out your local UID and GID and create a matching user inside the container
* Share your working source tree directory to the mock home directory inside the container
* Share your `~/.m2` directory with the container
    * This speeds things up as Maven dependencies won't get downloaded on each build
    * An alternative would be to figure out how to package them inside the container

### Default Build
To kick off a build run `<reflex-common repo>/build/dockerbuild.sh`

    [reflex-common] $ build/dockerbuild.sh
    Building reflex-common...SUCCESS!!
    Build log: build/docker-build.log

The command used to build the project is defined by the `DEFAULT_BUILD` variable in
`docker-entrypoint.sh`.

### Custom Build or Inspection
If you want to run another command or need to investigate issues building inside the container you
can append what you want to the `dockerbuild.sh` script.

**NOTE:**

* Whatever command is added will be executed in the base GIT directory of the reflex-common project
* If the added command has spaces then it needs to be wrapped in quotes


#### Examples

    [reflex-common] $ build/dockerbuild.sh pwd
    /home/$USERNAME/git/reflex-common
    [reflex-common] $ build/dockerbuild.sh "ls -alh"
    total 75K
    drwxr-xr-x 14 $USERNAME $USERNAME   21 Jan 19 20:02 .
    drwxr-xr-x  3 root      root         3 Jan 19 21:55 ..
    -rw-r--r--  1 $USERNAME $USERNAME  287 Jan 12 20:00 .classpath
    drwxr-xr-x  8 $USERNAME $USERNAME   16 Jan 19 21:55 .git
    -rw-r--r--  1 $USERNAME $USERNAME    8 Jan 19 20:02 .gitignore


If you want to interactively inspect the container just run `bash` as your added command.

    [build] $ ./dockerbuild.sh bash
    bash-4.2$ pwd
    /home/$USERNAME/git/reflex-common
    bash-4.2$ id
    uid=1000($USERNAME) gid=1000($USERNAME) groups=1000($USERNAME)


