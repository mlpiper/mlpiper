
from .constants import Constants

version = Constants.MLOPS_CURRENT_VERSION
__version__ = Constants.MLOPS_CURRENT_VERSION


from .versions import Versions
from .mlops import MLOps
from .mlops import mlops
from .mlops_devel import mlops_devel

from .stats_category import StatCategory
