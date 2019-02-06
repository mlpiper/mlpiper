#!/usr/bin/env python3
import json
import logging
import os
import sys

ERROR_RC = -1
SUCCESS_RC = 0


class ComponentVerifier:
    SUPPORTED_ENGINES = ["PySpark", "Tensorflow", "Python"]
    COMP_JSON_NAME = "component.json"
    MANIFEST_FILE_NAME = "manifest.json"
    MANIFEST_BUILTIN_KEY = "builtin"
    MANIFEST_EXAMPLE_KEY = "example"
    MANIFEST_COMP_PATH_KEY = "path"
    MANIFEST_COMP_VALIDATE_KEY = "validate"

    def __init__(self, path=None, log_level=logging.WARN):
        logging.basicConfig(format='%(levelname)s: %(message)s', level=log_level)

        self._components_root_path = os.path.dirname(os.path.realpath(__file__)) if path is None else path

        self._manifest = None
        manifest_filepath = os.path.join(self._components_root_path, ComponentVerifier.MANIFEST_FILE_NAME)
        try:
            if os.path.isfile(manifest_filepath):
                with open(manifest_filepath, "r") as f:
                    self._manifest = json.load(f)
                    logging.info("Read manifest successfully: " + manifest_filepath)
            else:
                logging.info(
                    "Manifest file was not found! All components will be verified! path: " + manifest_filepath)
        except Exception as e:
            raise Exception("Error while loading manifest file: [{}] {}".format(manifest_filepath, e))

    def _exit(self, msg):
        logging.error(msg)
        sys.exit(ERROR_RC)

    def verify(self):
        for engine in ComponentVerifier.SUPPORTED_ENGINES:
            logging.info("")
            logging.info("Verifying {} engine ...".format(engine))
            self._verify_engine_components(engine)

        print("ML components were verified successfully!")

    def _verify_engine_components(self, engine):
        engine_root = os.path.join(self._components_root_path, engine)
        for comp_dir in os.listdir(engine_root):

            comp_dir_fullpath = os.path.join(engine_root, comp_dir)
            if not os.path.isdir(comp_dir_fullpath):
                continue

            logging.info("Checking component: " + comp_dir)

            self._verify_component_dir(comp_dir_fullpath)

    def _verify_component_dir(self, comp_dir_fullpath):
        if not self._comp_in_manifest(comp_dir_fullpath):
            logging.info("Skip component verification: " + comp_dir_fullpath)
            return

        comp_json_fullpath = os.path.join(comp_dir_fullpath, ComponentVerifier.COMP_JSON_NAME)
        try:
            with open(comp_json_fullpath, "r") as f:
                comp_desc = json.load(f)

            logging.info("Component verified successfully: " + comp_dir_fullpath)
        except IOError:
            self._exit("Component json description file was not found! path: {}".format(comp_json_fullpath))
        except ValueError as e:
            self._exit("Invalid component json description file format! path: {}, error: {}"
                       .format(comp_json_fullpath, str(e)))

        try:
            comp_engine = comp_desc["engineType"]
            engine_dir = os.path.basename(os.path.dirname(comp_dir_fullpath))
            if comp_engine != engine_dir:
                self._exit("Component's engine does not match to the engine dir! comp-path: {}, "
                           "comp-engine: {}, engine-dir: {}".format(comp_dir_fullpath, comp_engine, engine_dir))

            comp_name = comp_desc["name"]
            comp_dir_name = os.path.basename(comp_dir_fullpath)
            if comp_name != comp_dir_name:
                self._exit("Component's dir name must be the same as the component's name! comp-path: {}, "
                           "comp-name: {}, dir-name: {}".format(comp_dir_fullpath, comp_name, comp_dir_name))

            program = comp_desc["program"]
            program_filepath = os.path.join(comp_dir_fullpath, program)
            if not os.path.isfile(program_filepath):
                self._exit("Missing component's main program! path: {}".format(program_filepath))
        except KeyError as e:
            self._exit("Missing required attributes in component json description file! comp-path: {}, error: {}"
                       .format(comp_json_fullpath, str(e)))

    def _comp_in_manifest(self, comp_dir_fullpath):
        if not self._manifest:
            # if not loaded, assume all components are in manifest
            return True

        comp_subdir = os.path.relpath(comp_dir_fullpath, self._components_root_path)

        for builtin in self._manifest[ComponentVerifier.MANIFEST_BUILTIN_KEY]:
            if comp_subdir == builtin[ComponentVerifier.MANIFEST_COMP_PATH_KEY]:
                return builtin.get(ComponentVerifier.MANIFEST_COMP_VALIDATE_KEY, True)

        if comp_subdir in self._manifest[ComponentVerifier.MANIFEST_EXAMPLE_KEY]:
            return True

        return False


if __name__ == "__main__":
    log_level = logging.INFO if len(sys.argv) > 1 and sys.argv[1] == "verbose" else logging.WARN
    ComponentVerifier(log_level=log_level).verify()
