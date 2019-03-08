from drycode.code_builder_base import CodeBuilderBase
from drycode.dry_defs import DryDefs


def python_file_to_class_name(file_name: str):
    """
    Convery a file name of the form xxx_yyy to class name XxxYyy
    :param file_name:
    :return: ClassName
    """
    parts = file_name.split("_")
    class_name = ""
    for p in parts:
        class_name += p.capitalize()
    return class_name


class PythonCodeBuilder(CodeBuilderBase):
    def __init__(self):
        super(PythonCodeBuilder, self).__init__()

    def generate_constant_class_code(self, dry_constant):
        code = ""
        code += "# " + self.get_auto_gen_str()
        if self._add_time_stamp:
            code += "# " + self.get_time_gen_str()
        code += "\n"

        code += "class {}:\n".format(dry_constant.name)

        for const in dry_constant.constants:
            value = dry_constant.constants[const]
            if isinstance(value, str):
                value = "\"" + value + "\""
            code += "    {} = {}\n".format(const, value)
        return code

    def generate_enum_class_code(self):
        code = ""
        return code
