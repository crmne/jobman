"""Helper code for implementing dbdict-compatible jobs"""
import inspect, sys, copy

#State values should be instances of these types:
INT = type(0)
FLT = type(0.0)
STR = type('')

COMPLETE = None    #jobs can return this by returning nothing as well
INCOMPLETE = True  #jobs can return this and be restarted

def subdict(dct, prefix):
    """Return the dictionary formed by keys in `dct` that start with the string `prefix`.

    In the returned dictionary, the `prefix` is removed from the keynames.
    Updates to the sub-dict are reflected in the original dictionary.

    Example:
        a = {'aa':0, 'ab':1, 'bb':2}
        s = subdict(a, 'a') # returns dict-like object with keyvals {'a':0, 'b':1}
        s['a'] = 5
        s['c'] = 9
        # a == {'aa':5, 'ab':1, 'ac':9, 'bb':2}

    """
    class SubDict(object):
        def __copy__(s):
            rval = {}
            rval.update(s)
            return rval
        def __eq__(s, other):
            if len(s) != len(other): 
                return False
            for k in other:
                if other[k] != s[k]:
                    return False
            return True
        def __len__(s):
            return len(s.items())
        def __str__(s):
            d = {}
            d.update(s)
            return str(d)
        def keys(s):
            return [k[len(prefix):] for k in dct if k.startswith(prefix)]
        def values(s):
            return [dct[k] for k in dct if k.startswith(prefix)]
        def items(s):
            return [(k[len(prefix):],dct[k]) for k in dct if k.startswith(prefix)]
        def update(s, other):
            for k,v in other.items():
                self[k] = v
        def __getitem__(s, a):
            return dct[prefix+a]
        def __setitem__(s, a, v):
            dct[prefix+a] = v

    return SubDict()

def subdict_copy(dct, prefix):
    return copy.copy(subdict(dct, prefix))

def call_with_kwargs_from_dict(fn, dct, logfile='stderr'):
    """Call function `fn` with kwargs taken from dct.

    When fn has a '**' parameter, this function is equivalent to fn(**dct).

    When fn has no '**' parameter, this function removes keys from dct which are not parameter
    names of `fn`.  The keys which are ignored in this way are logged to the `logfile`.  If
    logfile is the string 'stdout' or 'stderr', then errors are logged to sys.stdout or
    sys.stderr respectively.

    The reason this function exists is to make it easier to provide default arguments and 

    :param fn: function to call
    :param dct: dictionary from which to take arguments of fn
    :param logfile: log ignored keys to this file
    :type logfile: file-like object or string 'stdout' or string 'stderr'

    :returns: fn(**<something>)

    """
    argspec = inspect.getargspec(fn)
    argnames = argspec[0]
    if argspec[2] == None: #if there is no room for a **args type-thing in fn...
        kwargs = {}
        for k,v in dct.items():
            if k in argnames:
                kwargs[k] = v
            else:
                if not logfile:
                    pass
                elif logfile == 'stderr':
                    print >> sys.stderr, "WARNING: DictProxyState.call_substate ignoring key-value pair:", k, v
                elif logfile == 'stdout':
                    print >> sys.stdout, "WARNING: DictProxyState.call_substate ignoring key-value pair:", k, v
                else:
                    print >> logfile, "WARNING: DictProxyState.call_substate ignoring key-value pair:", k, v
        return fn(**kwargs)
    else:
        #there is a **args type thing in fn. Here we pass everything.
        return fn(**dct)

#MAKE YOUR OWN DBDICT-COMPATIBLE EXPERIMENTS IN THIS MODEL
def sample_experiment(state, channel):

    #read from the state to obtain parameters, configuration, etc.
    print >> sys.stdout, state.items()

    import time
    for i in xrange(100):
        time.sleep(1)
        # use the channel to know if the job should stop ASAP
        if channel() == 'stop':
            break

    # modify state to record results
    state['answer'] = 42

    #return either INCOMPLETE or COMPLETE to indicate that the job should be re-run or not.
    return COMPLETE

