import io
import json
import pkg_resources
import os
import sys

from parallelm.common.base import Base
from parallelm.common.mlcomp_exception import MLCompException
from parallelm.pipeline import json_fields


class ComponentsDesc(Base):

    CODE_COMPONETS_MODULE_NAME = 'parallelm.code_components'
    COMPONENT_JSON_FILE = "component.json"
    COMPONENT_METADATA_REF_FILE = "__component_reference__.json"

    def __init__(self, ml_engine=None, pipeline=None, comp_root_path=None, args=None):
        super(ComponentsDesc, self).__init__(ml_engine.get_engine_logger(self.logger_name()) if ml_engine else None)
        self._pipeline = pipeline
        self._comp_root_path = comp_root_path
        self._args = args

    @staticmethod
    def handle(args):
        ComponentsDesc(args).write_details()

    @staticmethod
    def next_comp_desc(root_dir):
        for root, _, files in os.walk(root_dir):
            for filename in files:
                comp_desc = ComponentsDesc._load_comp_desc(root, filename)
                if comp_desc:
                    yield root, comp_desc, filename

    @staticmethod
    def _load_comp_desc(root, filename):
        if filename.endswith(".json"):
            comp_json = os.path.join(root, filename)
            with io.open(comp_json, encoding="utf-8") as f:
                try:
                    comp_desc = json.load(f)
                except ValueError as ex:
                    raise MLCompException("Found json file with invalid json format! filename: {}, exception: {}".format(comp_json, str(ex)))

            if ComponentsDesc.is_valid(comp_desc):
                return comp_desc
        return None

    @staticmethod
    def is_valid(comp_desc):
        comp_desc_signature = [json_fields.COMPONENT_DESC_ENGINE_TYPE_FIELD,
                               json_fields.COMPONENT_DESC_NAME_FIELD,
                               json_fields.COMPONENT_DESC_LANGUAGE_FIELD,
                               json_fields.COMPONENT_DESC_PROGRAM_FIELD]

        if set(comp_desc_signature) <= set(comp_desc):
            return True
        return False

    def write_details(self):
        out_file_path = self._args.comp_desc_out_path if self._args.comp_desc_out_path \
            else './components_description.json'

        components_desc = self.load(extended=False)

        with io.open(out_file_path, mode='w', encoding="utf-8") as f:
            json.dump(components_desc, f, indent=4)
            print("Components details were written successfully to: " + out_file_path)

    def _add_default_values(self, comp_desc):
        if json_fields.COMPONENT_DESC_USER_STAND_ALONE not in comp_desc:
            comp_desc[json_fields.COMPONENT_DESC_USER_STAND_ALONE] = True

    def load(self, extended=True):
        components_desc = []

        if not self._comp_root_path:
            try:
                # The following call to 'pkg_resources.resource_filename' actually extract all the files
                # from the component's egg file from 'parallelm.code_components' folder
                self._comp_root_path = pkg_resources.resource_filename(ComponentsDesc.CODE_COMPONETS_MODULE_NAME, '')
                self._logger.info("Cached components are at: {}".format(self._comp_root_path))
            except ModuleNotFoundError:
                msg = "Either component's root path or component's egg file are missing!"
                self._logger.error(msg)
                raise MLCompException(msg)

        for comp_type in self._get_next_comp_type_in_pipeline():
            self._logger.info("Handling {}".format(comp_type))
            comp_path = os.path.join(self._comp_root_path, comp_type)

            if comp_path not in sys.path:
                sys.path.insert(0, comp_path)

            comp_desc = self.read_desc_file(comp_path)
            if extended:
                comp_desc[json_fields.COMPONENT_DESC_ROOT_PATH_FIELD] = comp_path

            self._add_default_values(comp_desc)
            self._logger.debug("Component loaded: " + str(comp_desc))
            components_desc.append(comp_desc)

        return components_desc

    def _get_next_comp_type_in_pipeline(self):
        comp_already_handled = []
        for pipe_comp in self._pipeline[json_fields.PIPELINE_PIPE_FIELD]:
            comp_type = pipe_comp[json_fields.PIPELINE_COMP_TYPE_FIELD]
            if comp_type in comp_already_handled:
                continue
            comp_already_handled.append(comp_type)
            yield comp_type

    def read_desc_file(self, comp_path):
        self._logger.debug("Reading component's metadata: {}".format(comp_path))
        comp_ref_json = os.path.join(comp_path, ComponentsDesc.COMPONENT_METADATA_REF_FILE)
        if os.path.isfile(comp_ref_json):
            with open(comp_ref_json, "r") as f:
                try:
                    comp_ref = json.load(f)
                except ValueError as e:
                    msg = "Failed to load (parse) component metadata's reference file! ref-file: {}".format(comp_ref_json)
                    self._logger.error(msg)
                    raise MLCompException(msg)

            metadata_filename = comp_ref[json_fields.COMPONENT_METADATA_REF_FILE_NAME_FIELD]
            comp_desc = ComponentsDesc._load_comp_desc(comp_path, metadata_filename)
        else:
            # Try to find any valid component's description file
            comp_desc_gen = ComponentsDesc.next_comp_desc(comp_path)
            try:
                # next() is called only once, because only one component JSON file is expected.
                _, comp_desc, _ = next(comp_desc_gen)
            except StopIteration:
                comp_desc = None

        if not comp_desc:
            msg = "Failed to find any valid component's json desc! comp_path: {}".format(comp_path)
            self._logger.error(msg)
            raise MLCompException(msg)

        return comp_desc

