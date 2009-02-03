"""Helper code for dbdict job drivers."""

import sys, inspect

from .experiment  import COMPLETE, INCOMPLETE, subdict

MODULE = 'dbdict_module'
SYMBOL = 'dbdict_symbol'
PREIMPORT = 'dbdict_preimport'

def dummy_channel(*args, **kwargs):
    return None

#
#this proxy object lets experiments use a dict like a state object
#
def DictProxyState(dct):
    """Convenient dict -> object interface for the state parameters of dbdict jobs.

    In the dbdict job running protocol, the user provides a job as a function with two
    arguments:
        
        def myjob(state, channel):
            a = getattr(state, 'a', blah)
            b = state.blah
            ...

    In the case that the caller of myjob has the attributes of this `state` in a dictionary,
    then this `DictProxyState` function returns an appropriate object, whose attributes are
    backed by this dictionary.

    """
    defaults_obj = [None]
    class Proxy(object):
        def substate(s, prefix=''):
            return DictProxyState(subdict(dct, prefix))

        def use_defaults(s, obj):
            """Use `obj` to retrieve values when they are not in the `dict`.

            :param obj: a dictionary of default values.
            """
            defaults_obj[0] = obj

        def __getitem__(s,a):
            """Returns key `a` from the underlying dict, or from the defaults.
            
            Raises `KeyError` on failure.
            """
            try:
                return dct[a]
            except Exception, e:
                try:
                    return getattr(defaults_obj[0], a)
                except:
                    raise e

        def __setitem__(s,a,v):
            """Sets key `a` equal to `v` in the underlying dict.  """
            dct[a] = v

        def __getattr__(s,a):
            """Returns value of key `a` from the underlying dict first, then from the defaults.

            Raises `AttributeError` on failure.
            """
            try:
                return dct[a]
            except KeyError:
                return getattr(defaults_obj[0], a)
        def __setattr__(s,a,v):
            dct[a] = v
    return Proxy()

def load_state_fn(state):

    #
    # load the experiment class 
    #
    dbdict_module_name = state[MODULE]
    dbdict_symbol = state[SYMBOL]

    preimport_list = state.get(PREIMPORT, "").split()
    for preimport in preimport_list:
        __import__(preimport, fromlist=[None], level=0)

    try:
        dbdict_module = __import__(dbdict_module_name, fromlist=[None], level=0)
        dbdict_fn = getattr(dbdict_module, dbdict_symbol)
    except:
        print >> sys.stderr, "FAILED to load job symbol:", dbdict_module_name, dbdict_symbol
        print >> sys.stderr, "PATH", sys.path
        raise
    
    return dbdict_fn


def run_state(state, channel = dummy_channel):
    fn = load_state_fn(state)
    rval = fn(state, channel) 
    if rval not in (COMPLETE, INCOMPLETE):
        print >> sys.stderr, "WARNING: INVALID job function return value"
    return rval

def call_with_kwargs_from_dict(fn, dct, logfile='stderr'):
    """Call function `fn` with kwargs taken from dct.

    When fn has a '**' parameter, this function is equivalent to fn(**dct).

    When fn has no '**' parameter, this function removes keys from dct which are not parameter
    names of `fn`.  The keys which are ignored in this way are logged to the `logfile`.  If
    logfile is the string 'stdout' or 'stderr', then errors are logged to sys.stdout or
    sys.stderr respectively.

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
        return fn(**s.subdict(prefix))

