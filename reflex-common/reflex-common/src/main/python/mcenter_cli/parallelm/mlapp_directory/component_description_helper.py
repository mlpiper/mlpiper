import pprint

from parallelm.mlapp_directory.mlapp_defs import ComponentDescriptionKeywords


class ComponentsDescriptionHelper(object):
    def __init__(self, components_info):
        self._comps = components_info
        self._by_engine = {}
        for comp in self._comps["components"]:
            engine_type = comp["engineType"]
            if engine_type not in self._by_engine:
                self._by_engine[engine_type] = {}
            self._by_engine[engine_type][comp["name"]] = comp

    def get_component(self, engine_type, component_name):
        if engine_type not in self._by_engine:
            return None
        if component_name not in self._by_engine[engine_type]:
            return None
        return self._by_engine[engine_type][component_name]

    def get_component_output_index_by_label(self, engine_type, component_name, output_label):
        component = self.get_component(engine_type, component_name)
        if component is None:
            raise Exception("Could not find component {}:{}".format(engine_type, component_name))
        output_info = component[ComponentDescriptionKeywords.COMPONENT_DESC_OUTPUT]
        idx = 0
        for out in output_info:
            if out[ComponentDescriptionKeywords.COMPONENT_DESC_OUTPUT_LABEL] == output_label:
                return idx
            idx += 1
        raise Exception("output label [{}] not found for component: {}:{}"
                        .format(output_label, engine_type, component_name))

    def get_component_output_label_by_index(self, engine_type, component_name, output_index):
        component = self.get_component(engine_type, component_name)
        if component is None:
            raise Exception("Could not find component {}:{}".format(engine_type, component_name))
        output_info = component[ComponentDescriptionKeywords.COMPONENT_DESC_OUTPUT]
        if output_index < 0 or output_index >= len(output_info):
            raise Exception("Error, output_indxe [{}] is out of range [0, {}] for component {}/{}"
                            .format(output_index, len(output_info) - 1, engine_type, component_name))

        return output_info[output_index][ComponentDescriptionKeywords.COMPONENT_DESC_OUTPUT_LABEL]

    def get_component_number_of_outputs(self, engine_type, component_name):
        component = self.get_component(engine_type, component_name)
        if component is None:
            raise Exception("Could not find component {}:{}".format(engine_type, component_name))
        return len(component[ComponentDescriptionKeywords.COMPONENT_DESC_OUTPUT])