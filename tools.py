import sys

from .experiment  import COMPLETE, INCOMPLETE

MODULE = 'dbdict_module'
SYMBOL = 'dbdict_symbol'

def dummy_channel(*args, **kwargs):
    return None

#
#this proxy object lets experiments use a dict like a state object
#
def DictProxyState(dct):
    class Proxy(object):
        def __getattr__(s,a):
            try:
                return dct[a]
            except KeyError:
                raise AttributeError(a)
        def __setattr__(s,a,v):
            try:
                dct[a] = v
            except KeyError:
                raise AttributeError(a)
    return Proxy()

def load_state_fn(state):

    #
    # load the experiment class 
    #
    dbdict_module_name = getattr(state,MODULE)
    dbdict_symbol = getattr(state, SYMBOL)

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

