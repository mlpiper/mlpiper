

class ComponentDescription:
    """
    Helper class to handle
    pipeline .json editing
    """
    def __init__(self, desc=None):
        self.argumentsKey = "arguments"
        self._desc = desc if desc is not None else {}

    @property
    def arguments(self):
        return self._desc["arguments"]

    @arguments.setter
    def arguments(self, value):
        self._desc["arguments"] = value

    @property
    def type(self):
        return self._desc["type"]

    @type.setter
    def type(self, value):
        self._desc["type"] = value
