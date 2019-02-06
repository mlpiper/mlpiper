MCenter client tools
-------------------------

This directory contains the MCenter client tools.
The MCenter client tools package provide the following abilities:

o Connect to a running MCenter server and upload/download/delete MLApps
o Run MLApps locally on the user development environment.


1 Installation
==============

1.1 Notes:
o MCenter cli require Python 3 to work.
o We strongly suggest to use a Python virtual environment to install MCenter cli into.
  But, you can also install directly into your standard Python environment.


1.2 Creating a a virtualenv:
--------------------------

o Create a new virtual env:
  > python3 -m virtualenv ~/my-venv (replace ~/my-venv with any path you like)

o Activate the virtual env:
  > . ~/my-venv/bin/activate


1.3 Installing MCenter cli:
------------------------

o Open the tarball:
 > tar xvf mcenter-cli-dist.tgz

o Cd into the directory:
 > cd mcenter mcenter-cli-dist

o Install the wheel files
 > python -m pip install mcenter_client_v0-0-py3-none-any.whl
 > python -m pip install mcenter_cli-1.2.3-py3-none-any.whl

o Run mcenter-cli --help to test that the client program is accessible and running correctly.
 > mcenter --help


2. Running code components locally:
===================================
TBD


3. Working with MLApps (download/upload/delete):
================================================
TBD


