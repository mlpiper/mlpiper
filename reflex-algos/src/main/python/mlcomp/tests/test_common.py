import os
from tempfile import mkstemp

from parallelm.common.buff_to_lines import BufferToLines


TMP_FILE_CONTENT = b"""
123 abc
4567 de fg

"""


class TestCommon:

    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def test_buff_2_lines(self):
        _, tmp_file = mkstemp(prefix='test_mlcomp_common', dir='/tmp')

        with open(tmp_file, "wb") as f:
            f.write(TMP_FILE_CONTENT)

        try:
            buff2lines = BufferToLines()
            with open(tmp_file, 'rb') as f:
                content = f.read()
                assert content == TMP_FILE_CONTENT
                buff2lines.add(content)

            raw_lines = TMP_FILE_CONTENT.decode().split('\n')
            for index, line in enumerate(buff2lines.lines()):
                assert line == raw_lines[index]

        finally:
            os.remove(tmp_file)
