#! /usr/bin/python

from __future__ import print_function

import sys
import yaml
import json
import os.path

known_files = {}


def process_yaml(filename, docloc, node):
    """
    Load specified yaml file and then resolve all $ref references.
    """
    filename = os.path.normpath(filename)
    for k, v in known_files.items():
        if k == filename or \
           (not v and os.path.basename(filename) == os.path.basename(k)):
            return {'$ref': '#%s%s' % (known_files[k], node)}
    assert not node
    with open(filename, 'r') as f:
        obj = yaml.load(f)
    known_files[filename] = docloc
    return iterover(obj, os.path.dirname(filename), docloc)


def iterover(obj, cwd, docloc):
    """
    Load external documents referenced by $ref.
    """
    if isinstance(obj, dict):
        ret = {}
        for k, v in obj.items():
            if k == '$ref':
                p = v.find('#')
                if p != -1:
                    fn = v[:p]
                    node = v[p + 1:]
                else:
                    fn = v
                    node = ''
                fn = os.path.join(cwd, fn)
                return process_yaml(fn, docloc, node)
            else:
                ret[k] = iterover(v, cwd, "%s/%s" % (docloc, k))
        return ret
    if isinstance(obj, list):
        ret = []
        for k, v in enumerate(obj):
            ret.append(iterover(v, cwd, "%s/%u" % (docloc, k)))
        return ret
    return obj

main_file_name = sys.argv[1]
main_yaml = process_yaml(main_file_name, '', '')
json.dump(main_yaml, sys.stdout, indent=2, sort_keys=True)
