
from parallelm.mlops import Versions, version
from parallelm.mlops.singelton import Singleton
from parallelm.mlops.mlops_exception import MLOpsException


class VersionInfo():
    def __init__(self, version, supported_versions, change_log):
        self.version = version
        self.supported_versions = supported_versions
        self.change_log = change_log


class VersionsInfo(object):
    def __init__(self):
        self._curr_version = version
        self._versions_info = {}

    def set_curr_version(self, curr_version):
        """
        For testing only !!!
        :param curr_version:
        :return:
        """
        self._curr_version = curr_version

    def get_curr_version(self):
        """
        :return: the current version of this library
        """
        return self._curr_version

    def register_version(self, version, supported_versions=[], change_log=None):
        if version in self._versions_info:
            raise MLOpsException("Version [{}] is already registered: cannot register the same version twice".format(
                version
            ))

        if not isinstance(supported_versions, list):
            raise MLOpsException("supported_versions argument should be a list")
        for supported_version in supported_versions:
            if supported_version not in self._versions_info:
                raise MLOpsException("Cannot register a supported version which does not exist [{}]".format(
                    supported_version
                ))

        self._versions_info[version] = VersionInfo(version, supported_versions, change_log)

    def verify_version_is_supported(self, version):
        if self._curr_version not in self._versions_info:
            raise MLOpsException("Current version [{}] is not registered".format(self._curr_version))

        # In case version is None then only verifying current version
        if version is None:
            return

        if version not in self._versions_info:
            raise MLOpsException("Version [{}] is unknown. Are you sure this is a correct version?".format(version))

        if version == self._curr_version:
            return

        current_version_info = self._versions_info[self._curr_version]
        if version not in current_version_info.supported_versions:
            raise MLOpsException("Requested version [{}] is not in current version [{}]. Supported versions are: {}".format(
                version, self._curr_version, current_version_info.supported_versions))


@Singleton
class VersionInfoSingleton(VersionsInfo):
    pass


mlops_version_info = VersionInfoSingleton.Instance()
mlops_version_info.register_version(Versions.VERSION_0_0_9, change_log="first mlops version")
mlops_version_info.register_version(Versions.VERSION_1_0_0, change_log="Complete restructure of API")
mlops_version_info.register_version(Versions.VERSION_1_0_1,
                                    [Versions.VERSION_1_0_0],
                                    change_log="Minor changes in API relative to 1.0.0")


def main():

    mlops_version_info.verify_version_is_supported("0.9.0")


if __name__ == "__main__":
    main()