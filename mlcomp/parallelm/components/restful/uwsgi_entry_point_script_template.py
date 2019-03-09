WSGI_ENTRY_SCRIPT = """
import logging
import uwsgi
from {module} import {cls}
from {restful_comp_module} import {restful_comp_cls}


logging.basicConfig(format='{log_format}')
logging.getLogger('{root_logger_name}').setLevel({log_level})

comp = {restful_comp_cls}(None)
comp.configure({params})

{cls}.uwsgi_entry_point(comp, '{pipeline_name}', '{model_path}', within_uwsgi_context=True)

application = {cls}._application
"""
