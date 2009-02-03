import sys, signal
from .tools import DictProxyState, load_state_fn, run_state

# N.B.
# don't put exotic imports here
# see what I did with def sql() below... since we shouldn't try to import that stuff unless we
# need it.

_experiment_usage = """Usage:
    dbdict-experiment <cmd>

Commands:

    help    Print help. (You're looking at it.)

    cmdline Obtain experiment configuration by evaluating the commandline
    sql     Obtain experiment configuration by querying an sql database

Help on individual commands might be available by typing 'dbdict-experiment <cmd> help'

"""
    #dbdict  Obtain experiment configuration by loading a dbdict file

def _dispatch_cmd(self, stack=sys.argv):
    try:
        cmd = stack.pop(0)
    except IndexError:
        cmd = 'help'
    try:
        fn = getattr(self, cmd)
    except AttributeError:
        print >> sys.stderr, "command not supported", cmd
        fn = self.help
    fn()

class RunCmdline(object):
    def __init__(self):
        try:
            dct = eval('dict(' + sys.argv.pop(0) + ')')
        except Exception, e:
            print >> sys.stderr, "Exception:", e
            self.help()
            return

        channel_rval = [None]

        def on_sigterm(signo, frame):
            channel_rval[0] = 'stop'

        #install a SIGTERM handler that asks the run_state function to return
        signal.signal(signal.SIGTERM, on_sigterm)
        signal.signal(signal.SIGINT, on_sigterm)

        def channel(*args, **kwargs):
            return channel_rval[0]

        run_state(dct, channel)
        print dct

    def help(self):
        print >> sys.stderr, "Usage: dbdict-experiment cmdline <config>"


class RunExperiment(object):
    """This class handles the behaviour of the dbdict-run script."""
    def __init__(self):
        exe = sys.argv.pop(0)
        #print 'ARGV', sys.argv
        _dispatch_cmd(self)

    def sql(self):
        from .dbdict_run_sql import run_sql
        return run_sql()

    cmdline = RunCmdline

    def help(self):
        print _experiment_usage # string not inlined so as not to fool VIM's folding mechanism
