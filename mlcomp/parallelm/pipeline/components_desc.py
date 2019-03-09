import json
import pkg_resources

from parallelm.common.base import Base
from parallelm.common.mlcomp_exception import MLCompException
from parallelm.pipeline import json_fields


class ComponentsDesc(Base):

    CODE_COMPONETS_MODULE_NAME = 'parallelm.code_components'
    COMPONENT_JSON_FILE = "component.json"
    COMPONENT_METADATA_REF_FILE = "__component_reference__.json"

    def __init__(self, ml_engine=None, args=None):
        super(ComponentsDesc, self).__init__(ml_engine.get_engine_logger(self.logger_name()) if ml_engine else None)
        self._args = args

    @staticmethod
    def handle(args):
        ComponentsDesc(args).write_details()

    def write_details(self):
        out_file_path = self._args.comp_desc_out_path if self._args.comp_desc_out_path \
            else './components_description.json'

        components_desc = self.load(extended=False)

        with open(out_file_path, 'w') as f:
            json.dump(components_desc, f, indent=4)
            print("Components details were written successfully to: " + out_file_path)

    def _add_default_values(self, comp_desc):
        if json_fields.COMPONENT_DESC_USER_STAND_ALONE not in comp_desc:
            comp_desc[json_fields.COMPONENT_DESC_USER_STAND_ALONE] = True

    def load(self, extended=True):
        components_desc = []
        for item in pkg_resources.resource_listdir(ComponentsDesc.CODE_COMPONETS_MODULE_NAME, ''):
            if not item.startswith('__') and \
                    pkg_resources.resource_isdir(ComponentsDesc.CODE_COMPONETS_MODULE_NAME, item):
                component_package = ComponentsDesc.CODE_COMPONETS_MODULE_NAME + '.' + item

                self._logger.debug("Loading component from egg resource: {}".format(component_package))
                components_desc_json = self.read_desc_file(component_package)
                try:
                    comp_desc = json.loads(components_desc_json)
                    if extended:
                        comp_desc[json_fields.COMPONENT_DESC_PACKAGE_FIELD] = component_package
                    self._add_default_values(comp_desc)
                    self._logger.debug("Component loaded: " + str(comp_desc))
                    components_desc.append(comp_desc)
                except ValueError as e:
                    self._logger.error("Failed to load component's json desc! err={}".format(e))
                    msg = "Failed to load component's json desc! err={}".format(e)
                    self._logger.error(msg)
                    raise MLCompException(msg)
        return components_desc

    def read_desc_file(self, component_package):
        if pkg_resources.resource_exists(component_package, ComponentsDesc.COMPONENT_METADATA_REF_FILE):
            comp_ref_json = pkg_resources.resource_string(component_package,
                                                          ComponentsDesc.COMPONENT_METADATA_REF_FILE)
            try:
                comp_ref = json.loads(comp_ref_json)
            except ValueError as e:
                msg = "Failed to load (parse) component metadata's reference file! err={}".format(e)
                self._logger.error(msg)
                raise MLCompException(msg)

            metadata_filename = comp_ref[json_fields.COMPONENT_METADATA_REF_FILE_NAME_FIELD]
        else:
            # Backward compatibility
            metadata_filename = ComponentsDesc.COMPONENT_JSON_FILE

        if not pkg_resources.resource_exists(component_package, metadata_filename):
            msg = "Component metadata file was not found: {}".format(metadata_filename)
            self._logger.error(msg)
            raise MLCompException(msg)

        return pkg_resources.resource_string(component_package, metadata_filename)
