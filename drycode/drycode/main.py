import argparse
import os

from drycode import DryCode
from drycode.lang_info import AllLangInfo
from drycode.object_helper import load_classes


def parse_args(all_lang):

    parser = argparse.ArgumentParser()

    parser.add_argument("--src", required=True, help="Source file or directory to use")
    parser.add_argument("--dst", required=True, help="Destination file or directory to use")
    parser.add_argument("--lang", required=True, help="Which language to generate code to")
    parser.add_argument("--verbose", action="store_true", help="Verbose mode")
    options = parser.parse_args()

    options.lang_list = options.lang.split(",")

    options.lang_list = map(lambda x: all_lang.get_by_option_str(x).code_lang, options.lang_list)
    return options


def detect_source_and_destination(src, dst):
    print("Detecting files in: {}".format(src))
    if not os.path.exists(src):
        raise Exception("src: {} does not exists".format(src))

    if os.path.isdir(src):

        if not os.path.isdir(dst):
            raise Exception("Source is a directory, but destination is not")

        print("Source is a directory getting all python files inside")
        py_files = list(filter(lambda x: x.endswith(".py"), os.listdir(src)))
        py_files = [os.path.join(src, py_file) for py_file in py_files]

        dst_files = [os.path.join(dst, os.path.basename(py_file)[:-3]) for py_file in py_files]

        return py_files, dst_files
    else:
        if os.path.isfile(src):
            return [src], [dst]
    raise Exception("src should be either a directory or a file")


def save_code_to_file(code, file_name):
    ff = open(file_name, "w")
    ff.write(code)
    ff.close()


def main():

    all_lang = AllLangInfo().register_all()
    options = parse_args(all_lang)

    print("Src:     {}".format(options.src))
    print("Dst:     {}".format(options.dst))
    print("Lang:    {}".format(options.lang_list))
    print("")

    dry = DryCode(all_lang)

    src_files, dst_files = detect_source_and_destination(options.src, options.dst)
    class_obj_list = load_classes(src_files)

    print("dst_file: {}".format(dst_files))
    for class_obj, dst_file in zip(class_obj_list, dst_files):
        for lang in options.lang_list:
            print("Generating {} {} {}".format(class_obj.__name__, dst_file, lang))

            code = dry.generate_constant_class_code(class_obj, lang)
            lang_dst = dst_file + all_lang.get_by_code_lang(lang).file_suffix
            print("Saving to {}".format(lang_dst))
            save_code_to_file(code, lang_dst)

            if options.verbose:
                print("------------")
                print(code)
                print("------------")


if __name__ == "__main__":
    main()
