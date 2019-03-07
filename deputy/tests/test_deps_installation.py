import subprocess

from parallelm.deputy.deps_install.py_package_installer import PyPackageInstaller
from parallelm.deputy.deps_install.r_package_installer import RPackageInstaller


class TestDepsInstallation:
    py_package_docopt = "docopt"
    py_deps = set(["requests==2.18.4", "%s==0.6.2" % py_package_docopt, "kazoo==2.4.0"])

    r_package_optparse = "optparse"
    r_deps = set([r_package_optparse, "reticulate"])

    @classmethod
    def setup_class(cls):
        try:
            subprocess.check_call("pip uninstall -y %s" % TestPythonDepsInstallation.py_package_docopt, shell=True)
            subprocess.check_call("R -e 'remove.packages(\"{}\")".format(TestDepsInstallation.r_package_optparse))
        except Exception:
            pass

    def test_py_packages_installation(self):
        PyPackageInstaller(TestDepsInstallation.py_deps).install()

        # Make sure an already installed packages are checked successfully
        PyPackageInstaller(TestDepsInstallation.py_deps).install()

    def test_py_empty_list(self):
        PyPackageInstaller(set()).install()

    def test_r_packages_installation(self):
        RPackageInstaller(TestDepsInstallation.r_deps).install()

        # Make sure an already installed packages are checked successfully
        RPackageInstaller(TestDepsInstallation.r_deps).install()

    def test_r_empty_list(self):
        RPackageInstaller(set()).install()


