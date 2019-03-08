from .drycode import DryCode
from drycode.code_lang import CodeLanguages

__import__('pkg_resources').declare_namespace(__name__)

from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)

project_name = "drycode"
version = "1.0.0"
