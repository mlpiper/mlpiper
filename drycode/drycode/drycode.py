"""
Dont Repeat Yourself Code generator

Generating constants + Enums from python to multiple other languages
"""

from collections import OrderedDict
import logging


class DryConstantClass:
    def __init__(self):
        self.name = None
        self.constants = OrderedDict()

    def set_from_obj(self, obj):
        self.name = obj.__name__
        methods = dir(obj)
        constant_vars = list(filter(lambda x: not x.startswith("__"), methods))
        for const in constant_vars:
            const_value = getattr(obj, const)
            self.constants[const] = const_value
        return self

    def __str__(self):
        s = "{}\n".format(self.name)
        for var in self.constants:
            s += "{}\n".format(var)
        return s


class DryEnumClass:
    def __init__(self):
        self.name = None
        self.constants = OrderedDict()

    def set_from_obj(self, obj):
        self.name = obj.__name__
        methods = dir(obj)
        constant_vars = list(filter(lambda x: not x.startswith("__"), methods))

        for const in constant_vars:
            const_value = getattr(getattr(obj, const), "value")
            self.constants[const] = const_value
        return self

    def __str__(self):
        s = "{}\n".format(self.name)
        for var in self.constants:
            s += "{}\n".format(var)
        return s


class DryCode:
    def __init__(self, all_lang_info):
        self._logger = logging.getLogger(DryCode.__name__)
        self._all_lang = all_lang_info
        self._add_time_stamp = True

    def add_time_stamp(self, show_time_stamp):
        self._add_time_stamp = show_time_stamp
        return self

    def generate_constant_class_code(self, class_obj, code_lang):
        self._logger.debug("Generating code for: {}".format(code_lang))
        dry_constants = DryConstantClass().set_from_obj(class_obj)
        builder = self._all_lang.get_by_code_lang(code_lang).code_gen
        self._logger.debug("Got builder: {}".format(builder))
        code = builder.add_time_stamp(self._add_time_stamp).generate_constant_class_code(dry_constants)
        return code

    def generate_enum_class_code(self, class_obj, code_lang):
        self._logger.debug("Generating code for: {}".format(code_lang))
        dry_enum = DryEnumClass().set_from_obj(class_obj)
        builder = self._all_lang.get_by_code_lang(code_lang).code_gen
        self._logger.debug("Got builder: {}".format(builder))
        code = builder.add_time_stamp(self._add_time_stamp).generate_enum_class_code(dry_enum)
        return code
