import pytest

import os
import uuid
import shutil
import filecmp

from parallelm.mlops.packer import DirectoryPack


def test_pack_unpack():
    test_folder = "test_dir"
    test_files = ["test_file1.txt",
                  "sub_folder1/test_file2.txt",
                  "sub_folder1/test_file3.txt",
                  "sub_folder2/sub_folder3/test_file4.txt"]

    # delete aftermods
    random_folder = str(uuid.uuid4())
    source_path = os.path.join(os.path.sep, "tmp", random_folder)
    dest_path = os.path.join(os.path.sep, "tmp", "dest-" + random_folder)
    os.mkdir(source_path)
    os.mkdir(dest_path)

    dir_to_pack = os.path.join(source_path, test_folder)

    for test_file in test_files:
        test_file_path = os.path.join(dir_to_pack, test_file)
        dir_name = os.path.dirname(test_file_path)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        f = open(test_file_path, 'w')
        f.write(test_file_path)
        f.close()

    dp = DirectoryPack()
    packed_file = dp.pack(dir_to_pack)
    dp.unpack(packed_file, dest_path)

    for root, dirs, files in os.walk(source_path):
        # for each dir and file resolve relative path,
        # - for dirs: check if such path exists in dest path
        # - for files: check if such path exists in dest path and compare the content
        for df in dirs + files:
            df_path = os.path.join(root, df)
            df_rel_path = os.path.relpath(df_path, source_path)
            other_path = os.path.join(dest_path, df_rel_path)

            assert os.path.exists(other_path)
            if os.path.isdir(df_path):
                assert os.path.isdir(other_path)
            if os.path.isfile(df_path):
                assert filecmp.cmp(df_path, other_path, shallow=False)

    shutil.rmtree(source_path)
    shutil.rmtree(dest_path)
    os.remove(packed_file)
