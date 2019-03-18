"""
Deputy main is the program which is launched in order to monitor
"""

import argparse
import logging
import sys

from parallelm.deputy.deputy import Deputy

LOG_LEVELS = {'debug': logging.DEBUG, 'info': logging.INFO, 'warn': logging.WARN, 'error': logging.ERROR}


def parse_args():
    parser = argparse.ArgumentParser(description="Parallelm Deputy")

    parser.add_argument('--logging-level', required=False, choices=list(LOG_LEVELS), default="info",
                        help="Set the logging level: {}".format(LOG_LEVELS.keys()))
    parser.add_argument('-f', '--pipeline-file', type=argparse.FileType('r'),
                        help='A json file path, whose content is a pipeline.')

    parser.add_argument('--python', default="/opt/conda/bin/python",
                        help="Path to python interpeter to use")
    parser.add_argument('--mlcomp-jar', default=None,
                        help="Path to mlcomp jar to be used for Java connected components")

    parser.add_argument('--use-color', action="store_true", default=False,
                        help="Use terminal colors when running pipeline")
    return parser.parse_args()


def set_logging_level(args):
    print("Setting logging level: {}".format(args.logging_level))
    if args.logging_level:
        logging.getLogger('parallelm').setLevel(LOG_LEVELS[args.logging_level])


def main():
    FORMAT = '%(asctime)-15s %(levelname)s [%(module)s:%(lineno)d]:  %(message)s'
    logging.basicConfig(format=FORMAT)

    args = parse_args()
    set_logging_level(args)
    deputy = Deputy().pipeline(args.pipeline_file).mlcomp_jar(args.mlcomp_jar).use_color(args.use_color)
    ret_val = deputy.run()
    sys.exit(ret_val)


if __name__ == '__main__':
    main()
