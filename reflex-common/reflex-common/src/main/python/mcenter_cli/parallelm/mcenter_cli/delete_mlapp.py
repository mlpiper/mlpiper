import logging
import pprint

from parallelm.mlapp_directory.mlapp_defs import MLAppProfileKeywords, MLAppPatternKeywords


class MLAppDeleteHelper:

    def __init__(self, mclient, mlapp_name, dry_run=True):
        """
        Deleting an mlapp given name.
        Note, If the mlapp is currently running deletion will fail.

        :param mclient: MCenterClient object to use to communicate with MCenter
        :param mlapp_name: mlapp_name to delete
        :param dry_run: do not delete just perform a dry run and show what is going to be deleted
        """
        self._logger = logging.getLogger(self.__class__.__name__)

        self._mlapp_name = mlapp_name
        self._mclient = mclient
        self._dry_run_mode = dry_run
        self._pattern_name = None
        self._pattern_id = None
        self._pipeline_patterns_ids = []
        self._pipeline_profiles_ids = []

    def _detect_all_ids(self):

        for profile_info in self._mclient.list_ion_profiles():
            self._logger.info("Profile part: [{}]".format(self._mlapp_name))
            self._logger.info(pprint.pformat(profile_info))

            profile_name = profile_info[MLAppProfileKeywords.NAME]
            if profile_name == self._mlapp_name:

                self._pattern_name = profile_info[MLAppProfileKeywords.PATTERN_NAME]
                self._pattern_id = profile_info[MLAppProfileKeywords.PATTERN_ID]
                self._profile_id = profile_info[MLAppProfileKeywords.ID]
                self._logger.info("Found mlapp {} {}".format(profile_name, self._profile_id))

                for node_info in profile_info[MLAppProfileKeywords.NODES]:
                    pipeline_pattern_id = node_info[MLAppProfileKeywords.NODE_PIPELINE_PATTERN_ID]
                    self._pipeline_patterns_ids.append(pipeline_pattern_id)

                    agent_set = node_info[MLAppProfileKeywords.NODE_PIPELINE_AGENT_SET]
                    for item in agent_set:
                        pipeline_profile_id = item[MLAppProfileKeywords.AGENT_SET_PIPELINE_PROFILE_ID]
                        self._pipeline_profiles_ids.append(pipeline_profile_id)
                return
        raise Exception("Could not find MLApp {}".format(self._mlapp_name))

    def _delete_mlapp(self):
        self._logger.info("Deleting profile: {}".format(self._profile_id))
        self._logger.info("Deleting pattern: {}".format(self._pattern_id))
        self._logger.info("Deleting pipeline profiles: {}".format(self._pipeline_profiles_ids))
        self._logger.info("Deleting pipeline patterns: {}".format(self._pipeline_patterns_ids))
        if not self._dry_run_mode:
            self._mclient.delete_ion_profile(self._profile_id)
            self._mclient.delete_ion_pattern(self._pattern_id)
            for pipeline_profile_id in self._pipeline_profiles_ids:
                self._mclient.delete_pipeline_profile(pipeline_profile_id)
            for pipeline_pattern_id in self._pipeline_patterns_ids:
                self._mclient.delete_pipeline_pattern(pipeline_pattern_id)

    def delete(self):
        """
        Perform actual delete.
        :return:
        """
        self._detect_all_ids()
        self._delete_mlapp()


