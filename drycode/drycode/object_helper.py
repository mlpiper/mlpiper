from importlib._bootstrap_external import SourceFileLoader


def detect_module_methods(module, prefix="test_"):
    mod_filtered_funcs = []
    mod_funcs = dir(module)
    for func in mod_funcs:
        if func.startswith(prefix):
            mod_filtered_funcs.append(func)
    return mod_filtered_funcs


def detect_module_classes(module):
    mod_dir = dir(module)
    # print("Mod dir: {}".format(mod_dir))
    module_class = list(filter(lambda x: not x.endswith("__") and not x.startswith("__"), mod_dir))
    if len(module_class) > 1:
        raise Exception("Only 1 class per module is supported for now")

    class_obj = getattr(module, module_class[0])
    return class_obj


def load_classes(src_files):

    class_list = []
    for src_file in src_files:
        #print("importing: {}".format(src_file))
        module = SourceFileLoader("", src_file).load_module()

        # module = importlib.import_module(src_file)
        class_list.append(detect_module_classes(module))
    return class_list