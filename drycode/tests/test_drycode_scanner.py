from pytest import fixture
import os
import shutil

from drycode.drycode_scanner import DryCodeScanner, CodeGenRequestInfo, detect_drycode_def_files
from drycode.lang_info import AllLangInfo
from drycode.object_helper import load_classes


@fixture(scope="function")
def datadir(tmpdir, request):
    """
    Each test module using this fixture will get the directory as the name of the module
    :param tmpdir:
    :param request:
    :return:
    """
    filename = request.module.__file__
    test_dir, _ = os.path.splitext(filename)

    tmpdir = os.path.join(tmpdir, request.function.__name__)
    if os.path.isdir(test_dir):
        shutil.copytree(test_dir, tmpdir)

    return tmpdir


class TestDryCodeScanner:

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    # @pytest.mark.skip(reason="no way of currently testing this")
    def test_basic(self, datadir):
        print(datadir)
        scanner = DryCodeScanner(datadir)
        code_gen_requests = scanner.scan()

        expected_list = [
            CodeGenRequestInfo('StringDefs', 'java', os.path.join(datadir, 'StringDefs.drycode.java')),
            CodeGenRequestInfo('string_defs', 'py', os.path.join(datadir, 'python/string_defs.drycode.py'))]

        assert code_gen_requests == expected_list

        code_gen_requests = scanner.fix_per_lang_settings(AllLangInfo.register_all())

    def test_detect_def_files(self, datadir):
        defs_dir = os.path.join(datadir, "defs")
        defs_files = detect_drycode_def_files(defs_dir)

        expected_def_files = ["defs/string_defs.py"]
        rel_path_def_files = [os.path.relpath(f, datadir) for f in defs_files]
        assert(expected_def_files == rel_path_def_files)
        class_obj_list = load_classes(defs_files)
        assert(class_obj_list[0].__name__ == "StringDefs")

    def test_generate(self, datadir):
        scanner = DryCodeScanner(datadir)
        scanner.scan()
        scanner.fix_per_lang_settings(AllLangInfo.register_all())

        defs_dir = os.path.join(datadir, "defs")
        defs_files = detect_drycode_def_files(defs_dir)
        class_obj_list = load_classes(defs_files)
        scanner.generate(class_obj_list)

        for item in scanner.items:
            assert(os.path.exists(item.dst_path))