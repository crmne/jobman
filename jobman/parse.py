from __future__ import with_statement

try:
    from functools import partial
except ImportError:
    from theano.gof.python25 import partial
        
import re
import os
from tools import UsageError, reval


def _convert(obj):
    try:
        return eval(obj, {}, {})
    except (NameError, SyntaxError):
        return obj

SPLITTER = re.compile('([^:=]*)(:=|=|::)(.*)')
def standard(*strings, **kwargs):
    converter = kwargs.get('converter', _convert)
    d = {}
    for string in strings:
        m = SPLITTER.match(string)
        if m is None:
            raise UsageError('Expected a keyword argument in place of "%s"' % (string,))
        k = m.group(1).strip()
        if m.group(2) == '=':
            v = converter(m.group(3).strip())
        elif m.group(2) == '::':
            k += '.__builder__'
            v = m.group(3).strip()
        elif m.group(2) == ':=':
            v = reval(m.group(3).strip())
        else:
            assert False # Forgot to add case to match re.
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

