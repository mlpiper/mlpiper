from parallelm.mlapp_directory.mlapp_from_directory_builder import MLAppFromDirectoryBuilder
from parallelm.mlapp_directory.mlapp_defs import MLAppProfileKeywords
from parallelm.mcenter_cli.delete_mlapp import MLAppDeleteHelper


def _is_mlapp_exists(mclient, mlapp_name):
    for profile_info in mclient.list_ion_profiles():
        profile_name = profile_info[MLAppProfileKeywords.NAME]
        if profile_name == mlapp_name:
            return True
        return False


def upload_mlapp_v2(mclient, mlapp_dir, force, ee=None):

    builder = MLAppFromDirectoryBuilder(mclient, mlapp_dir).set_ee(ee)
    mlapp_name = builder.get_mlapp_name()

    if _is_mlapp_exists(mclient, mlapp_name):
        if force:
            MLAppDeleteHelper(mclient, mlapp_name, dry_run=False).delete()
        else:
            raise Exception("MLApp [{}] exists, to override see help".format(mlapp_name))

    builder.build()
