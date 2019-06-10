
"""
Scan a directory recursively and detect components
"""

import fnmatch
import logging
import os
import re
import parallelm.common.constants as MLCompConstants

from parallelm.pipeline import json_fields
from parallelm.pipeline.components_desc import ComponentsDesc
from parallelm.pipeline.component_language import ComponentLanguage


class ComponentScanner(object):

    def __init__(self):
        pass

    def scan_dir(self, root_dir):
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
        for root, comp_desc, comp_filename in ComponentsDesc.next_comp_desc(root_dir):
            engine_type = comp_desc[json_fields.COMPONENT_DESC_ENGINE_TYPE_FIELD]
            comps.setdefault(engine_type, {})

            comp_name = comp_desc[json_fields.COMPONENT_DESC_NAME_FIELD]
            if comp_name in comps[engine_type]:
                raise Exception("Component already defined!\n\tPrev comp file: {}\n\tCurr comp file: {}"
                                .format(
                                    os.path.join(comps[engine_type][comp_name]["root"], comps[engine_type][comp_name]["comp_filename"]),
                                    os.path.join(root, comp_filename))
                                )

            comps[engine_type][comp_name] = {}
            comps[engine_type][comp_name]["comp_desc"] = comp_desc
            comps[engine_type][comp_name]["root"] = root
            comps[engine_type][comp_name]["files"] = self._include_files(root, comp_desc)
            # Always include current component json file regardless of its name.
            comps[engine_type][comp_name]["files"].append(comp_filename)
            comps[engine_type][comp_name]["comp_filename"] = comp_filename

            logging.debug("Found component, root: {}, engine: {}, name: ".format(root, engine_type, comp_name))
        return comps

    def _include_files(self, comp_root, comp_desc):
        include_patterns = self._parse_patterns(comp_desc.get(json_fields.COMPONENT_DESC_INCLUDE_GLOB_PATTERNS))
        exclude_patterns = self._parse_patterns(comp_desc.get(json_fields.COMPONENT_DESC_EXCLUDE_GLOB_PATTERNS))

        # Add "requirements.txt" if includeGlobPattern is defined,
        # so requirements file will be always copied.
        if len(include_patterns):
            if os.path.exists(os.path.join(comp_root, MLCompConstants.REQUIREMENTS_FILENAME)):
                include_patterns.append(MLCompConstants.REQUIREMENTS_FILENAME)

        included_files = []
        init_py_found = False
        for root, _, files in os.walk(comp_root):
            for f in files:
                rltv_path = os.path.relpath(root, comp_root)
                filepath = os.path.join(rltv_path, f) if rltv_path != "." else f
                if self._path_included(filepath, include_patterns, exclude_patterns):
                    if filepath == "__init__.py":
                        init_py_found = True

                    # There can be several comp JSONs in one folder.
                    # Don't include any of them, even related to current component,
                    # it will be included automatically
                    if ComponentsDesc._load_comp_desc(comp_root, f):
                        continue

                    included_files.append(filepath)

        if comp_desc[json_fields.COMPONENT_DESC_LANGUAGE_FIELD] == ComponentLanguage.PYTHON and not init_py_found:
            comp_name = comp_desc[json_fields.COMPONENT_DESC_NAME_FIELD]
            raise Exception("Missing '__init__.py' in component's root folder or it is not included"
                            " by 'glob' pattern! Please make sure to add it! name: {}, path: {}"
                            .format(comp_name, comp_root))

        return included_files

    def _parse_patterns(self, pattern):
        if not pattern:
            return []
        return re.sub("\s+", "", pattern.strip()).split('|')

    def _path_included(self, file, include_patterns, exclude_patterns):
        # For any given path, assume first that it should be included. This is the default
        # if no 'include' matcher exists. If 'include' matcher exists, assume that the path
        # should be excluded, then check the inclusion condition and set it accordingly
        included = False if include_patterns else True

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


