import jinja2

from drycode.code_builder_base import CodeBuilderBase
from drycode.dry_defs import DryDefs


def to_lower_case(var_name):
    return var_name.lower()


def to_upper_case(var_name):
    return var_name.upper()


ENUM_EXTRA_METHODS_TEMPLATE = \
"""
    public static {{class_name}} fromString(String name) {
        if (name != null) {
            for ({{class_name}} item : {{class_name}}.values()) {
                if (name.equalsIgnoreCase(item.name)) {
                    return item;
                }
            }
        }
        return null;
    }

    public static List<{{class_name}}> fromString(List<String> nameList) {
        List<{{class_name}}> itemList = new ArrayList<>();

        for (String name : nameList) {
            {{class_name}} item = fromString(name);
            if (item != null) {
                itemList.add(alertType);
            }
        }
        return itemList;
    }
"""


class JavaCodeBuilder(CodeBuilderBase):
    FINAL_CONST_PREFIX = "public static final String"

    def __init__(self):
        super(JavaCodeBuilder, self).__init__()

    def generate_constant_class_code(self, dry_constant):
        code = ""

        name = dry_constant.name
        code += "// " + self.get_auto_gen_str()
        if self._add_time_stamp:
            code += "// " + self.get_time_gen_str()
        code += "\n"

        # TODO: possiby transofrm the name to camel case ...
        code += "public class {} ".format(name) + "{\n"
        code += "\n"
        for const in dry_constant.constants:
            value = dry_constant.constants[const]
            if isinstance(value, str):
                value = "\"" + value + "\""
            code += "    {} {} = {};\n".format(JavaCodeBuilder.FINAL_CONST_PREFIX, const, value)
        code += "};\n"
        return code

    def generate_enum_class_code(self, dry_constant):
        code = ""
        name = dry_constant.name
        code += "// " + self.get_auto_gen_str()
        if self._add_time_stamp:
            code += "// " + self.get_time_gen_str()
        code += "\n"

        # TODO: possiby transofrm the name to camel case ...
        code += "public enum {} ".format(name) + "{\n"
        code += "\n"
        item_num = len(dry_constant.constants)
        idx = 0
        #print(item_num)
        for const in dry_constant.constants:
            value = dry_constant.constants[const]
            # print("Value = {}".format(value))
            var_name = to_upper_case(value)
            if isinstance(value, str):
                value = "\"" + value + "\""
            code += "\n    @SerializedName({})\n".format(value)
            if idx < item_num - 1:
                code += "    {} ({}),\n".format(var_name, value)
            else:
                code += "    {} ({});\n".format(var_name, value)
            idx += 1

        code += "\n"
        t = jinja2.Template(ENUM_EXTRA_METHODS_TEMPLATE)
        code += t.render(class_name=name) + "\n"
        code += "};\n"
        return code
