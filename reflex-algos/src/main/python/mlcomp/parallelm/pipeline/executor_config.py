
import collections

MLCOMP_JAR_ARG = "mlcomp_jar"
SPARK_JARS_ARG = "spark_jars"
SPARK_JARS_ENV_VAR = SPARK_JARS_ARG.upper()

ExecutorConfig = collections.namedtuple('ExecutorConfig', 'pipeline pipeline_file run_locally {}'
                                        .format(MLCOMP_JAR_ARG))
