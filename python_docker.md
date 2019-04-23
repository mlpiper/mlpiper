3.5 Updating the Python Docker
==============================

By default, ParallelM uses a Python Docker image with minimal external libraries intalled.
In this section, we descript how you can install additional libraries.

Step 1: Verify the Image Exists
---------------------------

Run `docker images` to find out if image exist. Please note image name and tag.

Example:

```
$ docker images
REPOSITORY                 TAG                 IMAGE ID            CREATED             SIZE
pm-python-r                latest              e1168fe8d217        2 minutes ago       2.17GB
parallelm/mcenter_agent    1.2.0-6748          0c9cc572775d        2 weeks ago         3.48GB
parallelm/mcenter_server   1.2.0-6748          c63bc2fd65b6        2 weeks ago         2.2GB
zookeeper                  latest              89f7884dcc4e        3 weeks ago         148MB
```

In this example, we will modify the `pm-python-r:latest` image.

Step 2: Create a Container from the Image
-----------------------------------------

Run the Docker interactively to create a shell that executes within the Docker. Use your own Docker image name and tag.
Example:

```
docker run -it pm-python-r:latest bash
```

The shell prompt appears:

```
root@c06e83c5757e:/#
```

Step 3: Install Libraries
-------------------------

From the shell, install the software you want. Here are some examples:

```
root@c06e83c5757e:/# pip install pymongo
Successfully installed pymongo-3.7.2
```

```
root@c06e83c5757e:/# pip2 install pymongo
Requirement already satisfied: pymongo in /usr/local/lib/python2.7/dist-packages

```

```
root@c06e83c5757e:/# apt-get update
root@c06e83c5757e:/# apt-get install mongo-tools
```

**Note: do not exit the container yet.**

Step 4: Commit the Updated Docker Image
---------------------------------------

From a command prompt outside of the running Docker, commit the new state of the Docker with same name and tag as you noted in Step 1.
Example where the Docker container ID from Step 3 is `c06e83c5757e`:

```
docker commit c06e83c5757e pm-python-r:latest
```

Now you can exit the container created in Step 3.