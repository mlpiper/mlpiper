from drycode.code_builder_base import CodeBuilderBase


class JavaScriptCodeBuilder(CodeBuilderBase):

    EXPORT_CONST_STR = "export const"

    def __init__(self):
        super(JavaScriptCodeBuilder, self).__init__()

    def generate_constant_class_code(self, dry_constant):
        code = ""
        name = dry_constant.name

        code += "# " + self.get_auto_gen_str()
        if self._add_time_stamp:
            code += "# " + self.get_time_gen_str()
        code += "\n"

        code += JavaScriptCodeBuilder.EXPORT_CONST_STR + " {} = ".format(name) + "{\n"
        for const in dry_constant.constants:
            value = dry_constant.constants[const]
            if isinstance(value, str):
                value = "'" + value + "'"
            code += " {} : {},\n".format(const, value)
        code += "};\n"
        return code

    def generate_enum_class_code(self):
        code = "dd"
        return code
