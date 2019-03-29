"""
Scan a directory recursively and detect components
"""

import fnmatch
import json
import logging
import os
import pprint

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
        def _parse_patterns(pattern):
            if not pattern:
                return []
            return pattern.replace(" ", "").split('|')

        def _is_path_included(file, include_patterns, exclude_patterns):
            # For any given path, assume first that it should be included. This is the default
            # if no 'include' matcher exists. If 'include' matcher exists, assume that the path
            # should be excluded, then check the inclusion condition and set it accordingly
            included = False if len(include_patterns) > 0 else True

            for pattern in include_patterns:
                if fnmatch.fnmatch(file, pattern):
                    included = True
                    break

            # For a any given path, only if it is supposed to be included, check for exclusion condition.
            if included:
                for pattern in exclude_patterns:
                    if fnmatch.fnmatch(file, pattern):
                        included = False
                        break

            return included

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

                    include_patterns = _parse_patterns(comp_desc.get(json_fields.COMPONENT_DESC_INCLUDE_GLOB_PATTERNS, None))
                    exclude_patterns = _parse_patterns(comp_desc.get(json_fields.COMPONENT_DESC_EXCLUDE_GLOB_PATTERNS, None))

                    include_files = [os.path.join(root, f) for f in files + subdirs if _is_path_included(f, include_patterns, exclude_patterns)]

                    # save component files absolute paths
                    comp_name = os.path.basename(root)
                    comps[engine_type][comp_name] = {}

                    include_files.append(os.path.join(root, comp_desc[json_fields.COMPONENT_DESC_PROGRAM_FIELD]))

                    # usually init should exist
                    init_path = os.path.join(root, "__init__.py")
                    if os.path.exists(init_path):
                        include_files.append(init_path)
                    # otherwise add a flag, so it will be created during files copying
                    else:
                        comps[engine_type][comp_name]["init"] = "__init__.py"

                    comps[engine_type][comp_name]["comp_json"] = os.path.join(root, ComponentsDesc.COMPONENT_JSON_FILE)
                    comps[engine_type][comp_name]["files"] = include_files

                except Exception as e:
                    logging.error("Component in directory: {} has non valid description. [{}]".format(root, e))
                    continue
            else:
                json_files = [f for f in files if os.path.splitext(f)[1] == ".json"]
                for f in json_files:
                    comp_json = os.path.join(root, f)
                    with open(comp_json) as f:
                        comp_desc = json.load(f)

                    if not ComponentsDesc.is_valid(comp_desc):
                        continue

                    engine_type = comp_desc[json_fields.PIPELINE_ENGINE_TYPE_FIELD]
                    if engine_type not in comps:
                        comps[engine_type] = {}

                    include_patterns = _parse_patterns(comp_desc.get(json_fields.COMPONENT_DESC_INCLUDE_GLOB_PATTERNS, None))
                    exclude_patterns = _parse_patterns(comp_desc.get(json_fields.COMPONENT_DESC_EXCLUDE_GLOB_PATTERNS, None))

                    include_files = [os.path.join(root, f) for f in files + subdirs if _is_path_included(f, include_patterns, exclude_patterns)]

                    comp_name = comp_desc[json_fields.COMPONENT_DESC_NAME_FIELD]
                    comps[engine_type][comp_name] = {}
                    include_files.append(os.path.join(root, comp_desc[json_fields.COMPONENT_DESC_PROGRAM_FIELD]))
                    comps[engine_type][comp_name]["comp_json"] = comp_json
                    comps[engine_type][comp_name]["files"] = include_files
                    comps[engine_type][comp_name]["init"] = "__init__.py"

        logging.debug(pprint.pformat(comps))
        return comps
