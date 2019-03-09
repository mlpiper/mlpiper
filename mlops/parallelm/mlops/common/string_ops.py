import re
import six


def mask_passwords(input):
    """
    The regex pattern covers both json string and python dictionaries. Note that an open quota
    in a key parameter can be either ' or "

    :return  a printable string with masked passwords
    """
    if isinstance(input, dict):
        input = str(input)
    elif not isinstance(input, six.string_types):
        raise Exception("Input type should be either 'str' or 'dict'! input: {}, type: {}"
                        .format(input, type(input)))

    match_pattern = r'(?i)([\'"][\w-]*password[\'"]\s*:\s*).*?(\s*[,}])'
    replace_pattern = r'\1"*****"\2'

    return re.sub(match_pattern, replace_pattern, input)
