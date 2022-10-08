import os
import tarfile
import csv
from parallelm.mlops.mlops_exception import MLOpsException



class UntarTimelineCapture:
    """This class handles the MCenter time capture file untar
    """
    def __init__(self, input_timeline_capture, tmpdir):
        """Initialized the parameters of the untar class."""
        self._file_names = []
        self._tmpdir = tmpdir
        self._extracted_dir = self._tmpdir + '/timeline-capture-export/'
        self._input_timeline_capture = input_timeline_capture
        self._timeline_capture = {}

    # ### Untar timeline_capture
    def untar_timeline_capture(self):
        """
        The function untars the timeline capture file to a local folder and saves the file names
         into a list and the files themselves into a dict

        :param self:
        :return:
        """
        try:
            print("self._input_timeline_capture", self._input_timeline_capture)
            print("self._tmpdir", self._tmpdir)
            with tarfile.open(self._input_timeline_capture) as tar_obj:
                def is_within_directory(directory, target):
                    
                    abs_directory = os.path.abspath(directory)
                    abs_target = os.path.abspath(target)
                
                    prefix = os.path.commonprefix([abs_directory, abs_target])
                    
                    return prefix == abs_directory
                
                def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                
                    for member in tar.getmembers():
                        member_path = os.path.join(path, member.name)
                        if not is_within_directory(path, member_path):
                            raise Exception("Attempted Path Traversal in Tar File")
                
                    tar.extractall(path, members, numeric_owner=numeric_owner) 
                    
                
                safe_extract(tar_obj, self._tmpdir)
        except Exception as e:
            print("Unable to open the timeline capture file")
            raise MLOpsException(e)
        self._file_names = os.listdir(self._extracted_dir)

        try:
            for file_name in self._file_names:
                if 'csv' in file_name:
                    with open(self._extracted_dir + file_name, 'r') as f:
                        reader = csv.reader(f)
                        parsed_list = list(reader)
                    self._timeline_capture[file_name] = parsed_list[1:-1]
                    self._timeline_capture[file_name].append(parsed_list[-1])
                    self._timeline_capture[file_name + 'header'] = parsed_list[0]
        except Exception as err:
            self._timeline_capture = {}
            raise MLOpsException(err)