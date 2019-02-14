
from .constants import Constants

version = Constants.MLOPS_CURRENT_VERSION
__version__ = Constants.MLOPS_CURRENT_VERSION

# TODO: switch to following
#project_name = Constants.OFFICIAL_NAME

# For now until name change
project_name = Constants.OFFICIAL_NAME

from .versions import Versions
from .mlops import MLOps
from .mlops import mlops
from .mlops_devel import mlops_devel

from .stats_category import StatCategory
