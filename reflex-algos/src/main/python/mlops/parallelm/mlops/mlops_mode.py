"""
The MLOps module can operate in different modes.
The ConnectionMode defines how the MLOps program should connect to the ParallelM service (MLOps):
 o AGENT is used when the program invoking the MLOps library is initiated by the ParallelM agent.
 o ATTACH is when attaching the MLOps program to an ION already created within MLOps.
 o STAND_ALONE is used when running an MLOps program without the ParallelM service.

Each connection mode may support different types of input and output channels.
The input channel defines how information flows from MLOps to the user code.
The output channel defines how information flows from the user code to MLOps.

Below is the list of supported combinations:

conn             out-channel  in-channel
====================================
STAND_ALONE      FILE          ---

ATTACH           PYTHON        REST

AGENT            PYTHON         REST
AGENT            PYSPARK        REST

"""


class MLOpsMode:
    """
    Operating modes for MLOps library.
    """

    STAND_ALONE = "stand_alone"     # MLOps is running in stand_alone mode, MLOps is not present
    ATTACH = "attach"               # MLOps is attaching to MLOps
    AGENT = "agent"                 # MLOps is running as an ION node by an MLOps agent

    @staticmethod
    def from_str(mode):
        if mode in (MLOpsMode.STAND_ALONE, MLOpsMode.ATTACH, MLOpsMode.AGENT):
            return mode
        raise Exception("Operation mode: [{}] is not supported".format(mode))


class InputChannel:
    REST = "rest"


class OutputChannel:
    FILE = "file"
    PYTHON = "python"
    PYSPARK = "pyspark"

    @staticmethod
    def from_str(mode):
        if mode in (OutputChannel.FILE, OutputChannel.PYTHON, OutputChannel.PYSPARK):
            return mode
        raise Exception("Output channel {} is not supported".format(mode))
