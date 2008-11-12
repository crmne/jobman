import sys

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
            filename = sys.argv.pop(0)
            symbol = sys.argv.pop(0)
            config = sys.argv.pop(0)
            assert filename != 'help'
        except:
            self.help()
            return

        job_module = __import__(filename, fromlist=[None], level=0)
        try:
            job_class = getattr(job_module, symbol)
        except:
            print >> sys.stderr, "failed to load job class:", filename, symbol
            raise
        class Conf(object):
            def __init__(self, dct):
                for k, v in dct.items():
                    setattr(self, k, v)
        conf = Conf(eval('dict(' + config + ')'))
        job = job_class(conf)

        job.start()
        job.run(lambda : None)

        print conf.__dict__

    def help(self):
        print "Usage: dbdict-experiment file <filename> <ExperimentName> <config>"


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
