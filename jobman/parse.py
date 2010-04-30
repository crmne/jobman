from __future__ import with_statement

try:
    from functools import partial
except ImportError:
    from theano.gof.python25 import partial
        
import re
import os
from tools import UsageError


def _convert(obj):
    try:
        return eval(obj, {}, {})
    except (NameError, SyntaxError):
        return obj


def standard(*strings, **kwargs):
    converter = kwargs.get('converter', _convert)
    d = {}
    for string in strings:
        s1 = re.split(' *= *', string, 1)
        s2 = re.split(' *:: *', string, 1)
        if len(s1) == 1 and len(s2) == 1:
            raise UsageError('Expected a keyword argument in place of "%s"' % s1[0])
        elif len(s2) == 2:
            k, v = s2
            k += '.__builder__'
        elif len(s1) == 2:
            k, v = s1
            v = converter(v)
        d[k] = v
    return d

_comment_pattern = re.compile('#.*')
def filemerge(*strings, **kwargs):
    lineparser = kwargs.get('lineparser', standard)
    state = {}
    def process(s, cwd = None, prefix = None):
        if '=' in s or '::' in s:
            d = lineparser(s)
            if prefix:
                d = dict(('%s.%s' % (prefix, k), v) for k, v in d.iteritems())
            state.update(d)
        elif '<-' in s:
            next_prefix, s = map(str.strip, s.split('<-', 1))
            param = '%s.%s' % (prefix, next_prefix)
            if not prefix:
                param = next_prefix
            process(s, cwd, param)
        else:
            if cwd:
                s = os.path.realpath(os.path.join(cwd, s))
            with open(s) as f:
                lines = [_comment_pattern.sub('', x) for x in map(str.strip, f.readlines())]
                for line in lines:
                    if line:
                        process(line, os.path.split(s)[0], prefix)
    for s in strings:
        process(s)
    return state

raw = partial(standard, converter = lambda x:x)

raw_filemerge = partial(filemerge, lineparser = raw)

