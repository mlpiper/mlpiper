#!/usr/bin/env python

import sys
import os
import platform
import subprocess

print("Installing mlops locally")
from parallelm.mlops import version, project_name


def install_package(package):

    pip_cmd = [sys.executable, "-m", "pip", "install",  "-U", "--no-color", "--disable-pip-version-check", package]
    cmd = "yes w | " + " ".join(pip_cmd)
    subprocess.call(cmd, shell=True)


def main():

    mlcomp_dir = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
    print("MLComp dir: {}".format(mlcomp_dir))

    dist_dir = os.path.join(mlcomp_dir, "dist")

    files = os.listdir(dist_dir)
    print(files)
    python_version = platform.python_version_tuple()
    wheel_file = "{}-{}-py{}-none-any.whl".format(project_name, version, python_version[0])
    wheel_path = os.path.join(dist_dir, wheel_file)

    if not os.path.exists(wheel_path):
        raise Exception("File: {} - does not exists - build wheel files".format(wheel_path))

    install_package(wheel_path)


if __name__ == "__main__":
    main()
