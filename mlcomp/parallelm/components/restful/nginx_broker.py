"""
For internal use only. The nginx broker class is designed to handle any 'nginx' related actions, such as
setup, configuration and execution
"""
import os
import subprocess
import platform
import re
import shutil

from parallelm.common.base import Base
from parallelm.components.restful import util
from parallelm.components.restful.nginx_conf_template import NGINX_SERVER_CONF_TEMPLATE
from parallelm.components.restful.constants import SharedConstants, ComponentConstants, NginxConstants
from parallelm.common.mlcomp_exception import MLCompException


class NginxBroker(Base):
    origin_nginx_conf_filepath_pattern = '{}/nginx.conf'
    new_nginx_conf_filepath_pattern = '{}/nginx.conf.new'

    def __init__(self, ml_engine, dry_run=False):
        super(NginxBroker, self).__init__()
        self.set_logger(ml_engine.get_engine_logger(self.logger_name()))
        self._dry_run = dry_run

    def setup_and_run(self, shared_conf, nginx_conf):
        self._logger.info("Setup 'nginx' service ...")
        self._verify_dependencies()
        self._generate_configuration(shared_conf, nginx_conf)
        self._run(shared_conf)
        return self

    def quit(self):
        if not self._dry_run:
            self._logger.info("Stopping 'nginx' service ...")
            try:
                subprocess.call(NginxConstants.STOP_CMD, shell=True)
            except:
                # Should catch any exception in order to avoid masking of other important errors in the system
                pass

    def _verify_dependencies(self):
        util.verify_tool_installation(NginxConstants.VER_CMD, NginxConstants.DEV_AGAINST_VERSION, self._logger)

    def _generate_configuration(self, shared_conf, nginx_conf):
        access_log_off = NginxConstants.ACCESS_LOG_OFF_CONFIG \
            if nginx_conf[NginxConstants.DISABLE_ACCESS_LOG_KEY] else ""

        conf_content = NGINX_SERVER_CONF_TEMPLATE.format(
            port=nginx_conf[ComponentConstants.PORT_KEY],
            sock_filepath=os.path.join(shared_conf[SharedConstants.TARGET_PATH_KEY],
                                       shared_conf[SharedConstants.SOCK_FILENAME_KEY]),
            access_log_off=access_log_off)

        platform_name = platform.platform()
        nginx_server_conf_filepath = self._server_conf_filepath(platform_name)

        self._logger.info("Writing nginx server configuration to ... {}".format(nginx_server_conf_filepath))
        with open(nginx_server_conf_filepath, 'w') as f:
            f.write(conf_content)

        if self._debian_platform(platform_name) or self._redhat_platform(platform_name):
            # Newer versions of nginx requires the folder sites-enabled in their installation folder,
            # in order to enable extended server configurations, which are configured in conf.d.
            # Apparently, on 'redhat' platforms the given folder does not exits after nginx installation.
            if not os.path.exists(NginxConstants.SERVER_ENABLED_DIR):
                self._fix_missing_sites_enabled_conf(NginxConstants.NGINX_ROOT)

            sym_link = os.path.join(NginxConstants.SERVER_ENABLED_DIR,
                                    NginxConstants.SERVER_CONF_FILENAME)
            if not os.path.isfile(sym_link):
                self._logger.info("Creating nginx server sym link ... {}".format(sym_link))
                os.symlink(nginx_server_conf_filepath, sym_link)

        self._logger.info("Done with _generate_configuration ...")

    def _fix_missing_sites_enabled_conf(self, nginx_root):
        origin_nginx_conf_filepath = NginxBroker.origin_nginx_conf_filepath_pattern.format(nginx_root)
        new_nginx_conf_filepath = NginxBroker.new_nginx_conf_filepath_pattern.format(nginx_root)

        fix_configuration = True
        pattern_conf_d = re.compile(r'^\s*include\s+{}/conf\.d/\*\.conf;\s*$'.format(nginx_root))
        pattern_sites_enabled = re.compile(r'^\s*include\s+{}/sites-enabled/.*;\s*$'.format(nginx_root))
        line_to_add = '    include {}/sites-enabled/*;\n'.format(nginx_root)

        with open(origin_nginx_conf_filepath, 'r') as fr:
            with open(new_nginx_conf_filepath, 'w') as fw:
                for line in fr:
                    fw.write(line)
                    group = pattern_conf_d.match(line)
                    if group:
                        fw.write(line_to_add)
                    elif pattern_sites_enabled.match(line):
                        # sites-enabled already configured! Close and remove new file"
                        fix_configuration = False
                        break

        if fix_configuration:
            shutil.copyfile(new_nginx_conf_filepath, origin_nginx_conf_filepath)

        if os.path.exists(new_nginx_conf_filepath):
            os.remove(new_nginx_conf_filepath)

        os.mkdir(NginxConstants.SERVER_ENABLED_DIR, 0o644)

    def _server_conf_filepath(self, platform_name):
        if self._debian_platform(platform_name):
            d = NginxConstants.SERVER_CONF_DIR_DEBIAN
        elif self._redhat_platform(platform_name):
            d = NginxConstants.SERVER_CONF_DIR_REDHAT
        elif self._macos_platform(platform_name):
            if not os.path.isdir(NginxConstants.SERVER_CONF_DIR_MACOS):
                if not os.path.isdir(NginxConstants.NGINX_ROOT_MACOS):
                    raise MLCompException("'{}' does not exist or not a directory. Is nginx installed?".format(NginxConstants.NGINX_ROOT_MACOS))
                os.mkdir(NginxConstants.SERVER_CONF_DIR_MACOS)
            d = NginxConstants.SERVER_CONF_DIR_MACOS
        else:
            raise MLCompException("Nginx cannot be configured! Platform is not supported: {}".format(platform_name))

        return os.path.join(d, NginxConstants.SERVER_CONF_FILENAME)

    def _redhat_platform(self, platform_name):
        return any(re.findall(NginxConstants.SUPPORTED_PLATFORMS_REDHAT, platform_name, re.IGNORECASE))

    def _debian_platform(self, platform_name):
        return any(re.findall(NginxConstants.SUPPORTED_PLATFORMS_DEBIAN, platform_name, re.IGNORECASE))

    def _macos_platform(self, platform_name):
        return NginxConstants.SUPPORTED_PLATFORMS_MACOS in platform_name.lower()

    def _run(self, shared_conf):
        self._logger.info("Starting 'nginx' service ... cmd: '{}'".format(NginxConstants.START_CMD))
        if self._dry_run:
            return

        rc = subprocess.check_call(NginxConstants.START_CMD, shell=True)
        if rc != 0:
            raise MLCompException("nginx service failed to start! It is suspected as not being installed!")

        self._logger.info("'nginx' service started successfully!")


if __name__ == '__main__':
    import logging
    import tempfile

    root_dir = '/tmp/nginx-test'

    if not os.path.isdir(root_dir):
        os.makedirs(root_dir)

    shared_conf = {
        SharedConstants.TARGET_PATH_KEY: tempfile.mkdtemp(prefix='restful-', dir=root_dir),
        SharedConstants.SOCK_FILENAME_KEY: 'restful_mlapp.sock'
    }

    nginx_conf = {
        ComponentConstants.HOST_KEY: 'localhost',
        ComponentConstants.PORT_KEY: 8888
    }

    print("Target path: {}".format(shared_conf[SharedConstants.TARGET_PATH_KEY]))

    logging.basicConfig()
    logger = logging.getLogger('NginxBroker')
    logger.setLevel(logging.INFO)
    NginxBroker(logger, dry_run=True).setup_and_run(shared_conf, nginx_conf)
