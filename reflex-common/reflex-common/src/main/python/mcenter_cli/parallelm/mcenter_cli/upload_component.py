
import tarfile
import os
import uuid


def upload_component(mcenter_client, comp_dir):
    """
    Uploading a component to mcenter
    :param mcenter_client:
    :param comp_dir:
    """

    component_tar_path = os.path.join("/tmp", "component.{}.tar".format(uuid.uuid4()))
    with tarfile.open(component_tar_path, "w:") as tar:
        tar.add(comp_dir, arcname=os.path.basename(comp_dir))
    try:
        mcenter_client.upload_component(component_tar_path, do_store=True, overwrite=True, file_type='tar')
    finally:
        os.remove(component_tar_path)
