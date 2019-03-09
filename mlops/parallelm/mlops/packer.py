import logging
import os
import tarfile
import uuid
from parallelm.mlops.mlops_exception import MLOpsException


class DirectoryPack:
    """
    Provides API to pack a folder as tar gz archive.
    This is an internal class which should not be exposed to users of MLOps.
    """
    def __init__(self, temp_folder="/tmp"):
        """
        Perform initialization of the DirectoryPack.
        :param temp_folder: folder to save a tar gz file
        :return:
        """
        self._logger = logging.getLogger(__name__)

        self.temp_file = str(uuid.uuid4()) + ".tar.gz"
        self.source_gzip = os.path.join(temp_folder, self.temp_file)

    def pack(self, source_dir_path):
        """
        Packs a folder
        :param source_dir_path: folder to pack
        :return: path to created tar gz file
        """
        if not (os.path.exists(source_dir_path) and os.access(source_dir_path, os.R_OK)):
            raise MLOpsException("Path: {} does not exist or not readable".format(source_dir_path))

        with tarfile.open(self.source_gzip, "w:gz") as tar:
            tar.add(source_dir_path, arcname=os.path.basename(source_dir_path))
        self._logger.info(
            "Directory was packed successfully! source={}, dest={}".format(source_dir_path, self.source_gzip))

        return self.source_gzip

    def unpack(self, arc_path, dest_dir_path):
        """
        Unpacks a tar gz archive into a provided folder
        :param arc_path: path to an archive file
        :param dest_dir_path: path to a destination folder.
        :return:
        """
        if not os.path.exists(arc_path):
            raise MLOpsException("Path: {} does not exist".format(arc_path))

        if not (os.path.exists(dest_dir_path) and os.access(dest_dir_path, os.W_OK)):
            raise MLOpsException("Path: {} does not exist or not writable".format(dest_dir_path))

        with tarfile.open(arc_path, "r:gz") as tar:
            tar.extractall(dest_dir_path)
        self._logger.info("g zip was extracted successfully! source={}, dest={}".format(arc_path, dest_dir_path))
