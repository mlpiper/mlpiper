"""
Scan a directory recursively and detect components
"""
import logging
import os
import pprint
import json

from parallelm.pipeline.components_desc import ComponentsDesc
from parallelm.pipeline import json_fields


class ComponentScanner(object):

    @classmethod
    def scan_dir(cls, root_dir):
        """
        Scanning a directory returning a map of components:
        {
            "name1": {
                "directory": path_relative_to_root_dir
            }
        }
        :return:
        """
        comps = {}

        logging.debug("Scanning {}".format(root_dir))
        for root, subdirs, files in os.walk(root_dir):
            logging.debug("root: {} subdirs: {}".format(root, subdirs))
            if ComponentsDesc.COMPONENT_JSON_FILE in files:
                try:
                    with open(os.path.join(root, ComponentsDesc.COMPONENT_JSON_FILE)) as f:
                        comp_desc = json.load(f)
                    engine_type = comp_desc[json_fields.PIPELINE_ENGINE_TYPE_FIELD]
                    if engine_type not in comps:
                        comps[engine_type] = {}

                    comps[engine_type][os.path.basename(root)] = root
                except Exception as e:
                    logging.error("Component in directory: {} has non valide description. [{}]".format(root, e))
                    continue
        logging.debug(pprint.pformat(comps))
        return comps
