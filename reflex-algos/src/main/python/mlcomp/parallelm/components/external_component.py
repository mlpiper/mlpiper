"""
This file in intended to be used by external component that can use python (like R + reticulate).
This will be a single place needed to be imported (thus the short name) in order to get the api functions to
work with connected components, and possibly more in the future.
"""

from parallelm.common.singleton import Singleton
from parallelm.components.connectable_external_component import ConnectableExternalComponent


# An instance of the MLOps to be used when importing the pm library.
@Singleton
class ConnectableExternalComponentSingleton(ConnectableExternalComponent):
    pass


mlcomp = ConnectableExternalComponentSingleton.Instance()
