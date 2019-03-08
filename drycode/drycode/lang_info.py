from collections import OrderedDict

from drycode import CodeLanguages
from drycode.python_code_builder import PythonCodeBuilder, python_file_to_class_name
from drycode.java_code_builder import JavaCodeBuilder
from drycode.java_script_code_builder import JavaScriptCodeBuilder


class LangInfo(object):
    def __init__(self, code_lang, option_str, file_suffix, code_gen, file_name_to_class_name):
        self.code_lang = code_lang
        self.option_str = option_str
        self.file_suffix = file_suffix
        self.code_gen = code_gen
        self.file_name_to_class_name = file_name_to_class_name

    def __str__(self):
        return "{} {} {}".format(self.option_str, self.code_lang, self.file_suffix)


class AllLangInfo(object):
    def __init__(self):
        self._by_code_lang = OrderedDict()
        self._by_option_str = OrderedDict()
        self._by_file_suffix = OrderedDict()

    def __str__(self):
        s = ""
        for code_lang in self._by_code_lang:
            s += str(self._by_code_lang[code_lang]) + "\n"

    def register_lang(self, lang_info):
        self._by_code_lang[lang_info.code_lang] = lang_info
        self._by_option_str[lang_info.option_str] = lang_info
        self._by_file_suffix[lang_info.file_suffix] = lang_info

    @classmethod
    def register_all(cls):
        all_lang = AllLangInfo()
        all_lang.register_lang(LangInfo(CodeLanguages.PYTHON,
                                        "python",
                                        "py",
                                        PythonCodeBuilder(),
                                        python_file_to_class_name))
        all_lang.register_lang(LangInfo(CodeLanguages.JAVA, "java", "java", JavaCodeBuilder(), None))
        all_lang.register_lang(LangInfo(CodeLanguages.JAVA_SCRIPT, "java-script", "js", JavaScriptCodeBuilder(), None))
        return all_lang

    def get_by_code_lang(self, code_lang):
        if code_lang in self._by_code_lang:
            return self._by_code_lang[code_lang]
        raise Exception("No such CodeLang: {}".format(code_lang))

    def get_by_option_str(self, option_str):
        if option_str in self._by_option_str:
            return self._by_option_str[option_str]
        raise Exception("No such options str: {}".format(option_str))

    def get_by_file_suffix(self, file_suffix: str) -> LangInfo:
        if file_suffix in self._by_file_suffix:
            return self._by_file_suffix[file_suffix]
        raise Exception("No such suffix: {}".format(file_suffix))


