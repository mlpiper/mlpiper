#!/usr/bin/env python
import os
import glob
import shutil


def copy_protobuf_sources():
    curr_scirpt_dir = os.path.dirname(os.path.abspath(__file__))
    src_protobuf_dir = os.path.join(curr_scirpt_dir,
                                    '../../../../../reflex-common/reflex-common/target/generated-sources/protobuf')
    dst_protobuf_dir = os.path.join(curr_scirpt_dir, 'parallelm/protobuf')

    if not os.path.isdir(src_protobuf_dir):
        raise OSError(2, 'No such directory', src_protobuf_dir)

    # Cleanup destination if exists
    if os.path.exists(dst_protobuf_dir):
        shutil.rmtree(dst_protobuf_dir)

    os.makedirs(dst_protobuf_dir)

    # Copy protobuf files
    py_files = glob.glob(os.path.join(src_protobuf_dir, '*.py'))
    if not py_files:
        raise OSError(2, 'Generated protobuf python files were not found in directory!', src_protobuf_dir)

    for f in py_files:
        shutil.copyfile(f, os.path.join(dst_protobuf_dir, os.path.basename(f)))

    # Generate __init__.py
    with open(os.path.join(dst_protobuf_dir, '__init__.py'), 'w') as f:
        f.write("")


if __name__ == '__main__':
    copy_protobuf_sources()
