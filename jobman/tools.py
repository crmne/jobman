from __future__ import with_statement
import os
import re
import sql
import copy

################################################################################
### misc
################################################################################

class DD(dict):
    def __getattr__(self, attr):
        if attr == '__getstate__':
            return super(DD, self).__getstate__
        elif attr == '__slots__':
            return super(DD, self).__slots__
        return self[attr]
    def __setattr__(self, attr, value):
        # Safety check to ensure consistent behavior with __getattr__.
        assert attr not in ('__getstate__', '__slots__')
        self[attr] = value
    def __str__(self):
        return 'DD%s' % dict(self)
    def __repr__(self):
        return str(self)
    def __deepcopy__(self, memo):
        z = DD()
        for k,kv in self.iteritems():
            z[k] = copy.deepcopy(kv)
        return z

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
        if isinstance(obj, (str, int, float, list, tuple)) or obj in (True, False, None):
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
        elif len(s2) == 2:
            k, v = s2
            k += '.__builder__'
        elif len(s1) == 2:
            k, v = s1
            v = convert(v)
        d[k] = v
    return d

_comment_pattern = re.compile('#.*')
def parse_files(*files):
    state = {}
    def process(file, cwd = None, prefix = None):
        if '=' in file or '::' in file:
            d = parse(file)
            if prefix:
                d = dict(('%s.%s' % (prefix, k), v) for k, v in d.iteritems())
            state.update(d)
        elif '<-' in file:
            next_prefix, file = map(str.strip, file.split('<-', 1))
            process(file, cwd, '%s.%s' % (prefix, next_prefix) if prefix else next_prefix)
        else:
            if cwd:
                file = os.path.realpath(os.path.join(cwd, file))
            with open(file) as f:
                lines = [_comment_pattern.sub('', x) for x in map(str.strip, f.readlines())]
                for line in lines:
                    if line:
                        process(line, os.path.split(file)[0], prefix)
    for file in files:
        process(file)
    return state

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


################################################################################
### Helper functions operating on experiment directories
################################################################################


def find_conf_files(cwd, fname='current.conf', recurse=True):
    """
    This generator will iterator from the given directory, and find all job
    configuration files recursively (if specified). Job config files are read
    and a dict is returned with the proper key/value pairs.

    @param cwd: diretory to start iterating from
    @param fname: name of the job config file to look for and parse
    @param recurse: enable recursive search of job config files
    """
    for jobid in os.listdir(cwd):
        e = os.path.join(cwd, jobid)

        if os.path.isdir(e) and recurse:
            find_conf_files(e, fname)
                
        try:
            e_config = open(os.path.join(e, fname),'r')
        except:
            e_config = None

        if e_config:
            data = e_config.read().split('\n') 
            # trailing \n at end of file creates empty string
            jobdd = DD(parse(*data[:-1]))

            try:
                jobid = int(jobid)
            except ValueError:
                jobid = None
            yield (jobid, jobdd)


def rebuild_DB_from_FS(db, cwd='./', keep_id=True, verbose=False):
    """
    This method is meant to rebuild a database from the contents of the .conf
    files stored in an experiment directory. This can be useful for consolidating
    data stored in multiple locations or after bad things happen to the DB...

    @param db: db handle (as returned by sql.db) of the DB in which to insert job dicts
    @param cwd: current working dir or path from which to start looking for conf files
    @param keep_id: attempt to use the directory name as job id for inserting in DB
    @param verbose: prints info about which jobs are succesfully inserted
    """
    tot = 0
    for (jobid, jobdd) in find_conf_files(cwd):
        if keep_id and jobid:
            jobdd[sql.JOBID] = jobid
        status = sql.insert_dict(jobdd, db) 
        if status: tot += 1

        if verbose:
            print '** inserted job %i **' % jobdd[sql.JOBID]

    if verbose:
        print '==== Inserted %i jobs ====' % tot
