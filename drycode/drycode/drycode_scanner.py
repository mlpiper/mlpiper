import os
import re
from typing import List

from drycode.lang_info import AllLangInfo
from drycode.drycode import DryCode, DryConstantClass, DryEnumClass


class CodeGenRequestInfo:
    def __init__(self, class_name, file_suffix, path):
        self.path = path
        self.class_name = class_name
        self.file_suffix = file_suffix
        self.lang = None
        self.dst_path = None
        self.done = False
        self.error = None

    def __str__(self):
        return "class: {} lang: {} suffix: {} path: {}".format(self.class_name, self.lang, self.file_suffix, self.path)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


def detect_drycode_def_files(dir_path):
    if not os.path.exists(dir_path):
        raise Exception("src: {} does not exists".format(dir_path))

    if os.path.isdir(dir_path):
        py_files = list(filter(lambda x: x.endswith(".py"), os.listdir(dir_path)))
        py_files = [os.path.join(dir_path, py_file) for py_file in py_files]
        return py_files
    raise Exception("dir_path should be a directory")


class DryCodeScanner:
    """
    Scanning a directory recursively and finding all files of the form
    XXXX.drycode.YYYY where XXXX is the class name, and YYYY is the language

    For example EnumClass.drycode.java

    The return value will be a list of CodeGenRequestInfo objects
    """

    def __init__(self, dir_to_scan):
        self._drycode_token = "drycode"
        self._dir = dir_to_scan
        self._items = []

    @property
    def items(self):
        return self._items

    def scan(self):
        file_list = []
        re_expression = '.*[.]{}[.].*'.format(self._drycode_token)
        p = re.compile(re_expression, re.IGNORECASE)
        for root, subfolders, files in os.walk(self._dir):
            for f in files:
                m = p.match(f)
                if m:
                    file_list.append(os.path.join(root, f))

        for f in file_list:
            file_name = os.path.basename(f)

            m = re.match("^(.*)[.]{}[.](.*)$".format(self._drycode_token), file_name)
            if not m:
                raise Exception("Found file not in expected pattern.... {}".format(file_name))
            self._items.append(CodeGenRequestInfo(m.group(1), m.group(2), f))
        return self._items

    def fix_per_lang_settings(self, all_langs: AllLangInfo) -> List[CodeGenRequestInfo]:
        """

        :param all_langs:
        :return:
        """
        for item in self._items:
            lang_info = all_langs.get_by_file_suffix(item.file_suffix)
            item.lang = lang_info.code_lang
            if lang_info.file_name_to_class_name:
                item.class_name = lang_info.file_name_to_class_name(item.class_name)

            parts = os.path.basename(item.path).split(".")
            item.dst_path = os.path.join(os.path.dirname(item.path), "{}.{}".format(parts[0], item.file_suffix))
        return self._items

    @staticmethod
    def save_code_to_file(code, file_name):
        ff = open(file_name, "w")
        ff.write(code)
        ff.close()

    def generate(self, class_list):

        class_dict = {}
        for cls in class_list:
            class_dict[cls.__name__] = cls

        dry = DryCode(AllLangInfo.register_all()).add_time_stamp(False)
        not_defined = 0
        for item in self._items:
            # print("Handing item: {}".format(item))

            if item.class_name in class_dict:
                cls_obj = class_dict[item.class_name]
                # print("Found class object ")
                code = dry.generate_constant_class_code(cls_obj, item.lang)
                self.save_code_to_file(code, item.dst_path)
                item.done = True
            else:
                not_defined += 1
                item.error = "Could not find class definition: {}".format(item.class_name)
