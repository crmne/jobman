
import re

################################################################################
### misc
################################################################################

class DD(dict):
    def __getattr__(self, attr):
        return self[attr]
    def __setattr__(self, attr, value):
        self[attr] = value
    def __str__(self):
        return 'DD%s' % dict(self)
    def __repr__(self):
        return str(self)

def defaults_merge(d, defaults):
    for k, v in defaults.iteritems():
        if isinstance(v, dict):
            defaults_merge(d.setdefault(k, DD()), v)
        else:
            d.setdefault(k, v)


################################################################################
### resolve
################################################################################

def resolve(name, try_import=True):
    """
    Resolve a string of the form X.Y...Z to a python object by repeatedly using getattr, and
    __import__ to introspect objects (in this case X, then Y, etc. until finally Z is loaded).

    """
    symbols = name.split('.')
    builder = __import__(symbols[0])
    try:
        for sym in symbols[1:]:
            try:
                builder = getattr(builder, sym)
            except AttributeError, e:
                if try_import:
                    __import__(builder.__name__, fromlist=[sym])
                    builder = getattr(builder, sym)
                else:
                    raise e
    except (AttributeError, ImportError), e:
        raise type(e)('Failed to resolve compound symbol %s' % name, e)
    return builder

################################################################################
### dictionary
################################################################################

def convert(obj):
    try:
        return eval(obj, {}, {})
    except (NameError, SyntaxError):
        return obj

def flatten(obj):
    """nested dictionary -> flat dictionary with '.' notation """
    d = {}
    def helper(d, prefix, obj):
        if isinstance(obj, (str, int, float, list, tuple)):
            d[prefix] = obj #convert(obj)
        else:
            if isinstance(obj, dict):
                subd = obj
            else:
                subd = obj.state()
                subd['__builder__'] = '%s.%s' % (obj.__module__, obj.__class__.__name__)
            for k, v in subd.iteritems():
                pfx = '.'.join([prefix, k]) if prefix else k
                helper(d, pfx, v)
    helper(d, '', obj)
    return d

def expand(d):
    """inverse of flatten()"""
    #def dd():
        #return DD(dd)
    struct = DD()
    for k, v in d.iteritems():
        if k == '':
            raise NotImplementedError()
        else:
            keys = k.split('.')
        current = struct
        for k2 in keys[:-1]:
            current = current.setdefault(k2, DD())
        current[keys[-1]] = v #convert(v)
    return struct

def realize(d):
    if not isinstance(d, dict):
        return d
    d = dict((k, realize(v)) for k, v in d.iteritems())
    if '__builder__' in d:
        builder = resolve(d.pop('__builder__'))
        return builder(**d)
    return d

def make(d):
    return realize(expand(d))

################################################################################
### errors
################################################################################

class UsageError(Exception):
    pass

################################################################################
### parsing and formatting
################################################################################

def parse(*strings):
    d = {}
    for string in strings:
        s1 = re.split(' *= *', string, 1)
        s2 = re.split(' *:: *', string, 1)
        if len(s1) == 1 and len(s2) == 1:
            raise UsageError('Expected a keyword argument in place of "%s"' % s1[0])
        elif len(s1) == 2:
            k, v = s1
            v = convert(v)
        elif len(s2) == 2:
            k, v = s2
            k += '.__builder__'
        d[k] = v
    return d

def format_d(d, sep = '\n', space = True):
    d = flatten(d)
    pattern = "%s = %r" if space else "%s=%r"
    return sep.join(pattern % (k, v) for k, v in d.iteritems())

def format_help(topic):
    if topic is None:
        return 'No help.'
    elif isinstance(topic, str):
        help = topic
    elif hasattr(topic, 'help'):
        help = topic.help()
    else:
        help = topic.__doc__
    if not help:
        return 'No help.'

    ss = map(str.rstrip, help.split('\n'))
    try:
        baseline = min([len(line) - len(line.lstrip()) for line in ss if line])
    except:
        return 'No help.'
    s = '\n'.join([line[baseline:] for line in ss])
    s = re.sub(string = s, pattern = '\n{2,}', repl = '\n\n')
    s = re.sub(string = s, pattern = '(^\n*)|(\n*$)', repl = '')

    return s
