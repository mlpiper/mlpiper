#!/usr/bin/env python

import glob
import sys
import os
import platform
import subprocess


def install_package(package):

    pip_cmd = [sys.executable, "-m", "pip", "install",  "-U", "--disable-pip-version-check", package]
    cmd = "yes w | " + " ".join(pip_cmd)
    subprocess.call(cmd, shell=True)


def main():
    print("Installing mlcomp locally")

    mlcomp_dir = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
    print("MLComp dir: {}".format(mlcomp_dir))

    dist_dir = os.path.join(mlcomp_dir, "dist")

    files = os.listdir(dist_dir)
    print(files)
    python_version = platform.python_version_tuple()
    potential_wheels = glob.glob(os.path.join(dist_dir, "*-py{}*.whl".format(python_version[0])))
    if not potential_wheels:
        raise Exception("Wheel file not exist in: {}".format(dist_dir))

    if len(potential_wheels) != 1:
        raise Exception("Unexpected number of wheels: {}".format(potential_wheels))

    install_package(potential_wheels[0])


if __name__ == "__main__":
    main()
