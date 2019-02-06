# REFLEX-COMMON Build Container
[Packer](https://www.packer.io/) allows us to create reproducible build environments for REFLEX-COMMON.

## Requirements
* Internet connection
* [Docker](https://docs.docker.com/engine/installation/) installation
* [Packer](https://www.packer.io/docs/installation.html) installation

## Container Build Template
The `pm-reflex-common-builder.json.template` defines how the build container should be built. This template
will be rendered by the `create.pm-reflex-common-builder.sh` script.

### Builders Section
This section defines what type of container we would like to build. For now, we are only building
Docker containers but in the future we may want to expand to others
(e.g. [EC2](https://www.packer.io/docs/builders/amazon.html)).

### Provisioners Section
This section defines what to do inside the container in order to support building the REFLEX-COMMON platform.

### Post-Processors Section
This section defines what to do with the container after it has been built. For now, we are simply
uploading and tagging it in the **local Docker registry** on whichever system it's being built on.

## Create Build Environment
### Local Development
The `create.pm-reflex-common-builder.sh` script will create a container and make it available to
the locally installed Docker environment. Once a container is created there should not be any need
to run it again **unless** the build dependencies change. By default, the container will be tagged
as *latest*.

#### Container creation
1. Clone the REFLEX-COMMON repository
2. Go to the `build/packer` directory
3. Run `create.pm-reflex-common-builder.sh`
4. Verify that the image is available locally

        $ docker images
        REPOSITORY          TAG                 IMAGE ID            CREATED             SIZE
        pm-reflex-common-builder      latest              66bf8e483cdd        About an hour ago   443.3 MB
        <...>

### Official Releases
**TODO**

* Setup a central Docker registry to store official images
* Tie container creation into official release process/scripts and tag containers with the
release version
* Update build scripts to reflect container tag that matches the release version

