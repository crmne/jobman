"""Provides analyze command.
"""
import os
from .runner import runner_registry
from optparse import OptionParser
import logging
_logger = logging.getLogger('jobman.analyze_runner')

parser_analyze = OptionParser(usage = '%prog analyze [options] <cmdname>',
        description = "Analyze/modify the jobs in an experiment.  Try <cmdname> 'help' to see"
        " a list of commands",
        add_help_option=True)
parser_analyze.add_option('--extra', dest = 'extra', 
        type = 'str', default = '',
        help = 'comma-delimited list of extra imports for additional commands')
parser_analyze.add_option('--addr', dest = 'addr',
        type = 'str', default = 'pkl://'+os.getcwd(),
        help = 'Address of experiment root (starting with format prefix such'
        ' as postgres:// or pkl:// or dd://')
def runner_analyze(options, cmdname, *other_args):
    """Analyze the state/results of an experiment

    Example usage:

        jobman analyze --extra=jobs --addr=pkl://relpath/to/experiment <cmd>
        jobman analyze --extra=jobs --addr=pkl:///abspath/to/experiment <cmd>
        jobman analyze --extra=jobs --addr=postgres://user@host:dbname/tablename <cmd>

    Try jobman analyze help for more information.
    """
    #parse the address
    if options.addr.startswith('pkl://'):
        import analyze.pkl
        exproot = options.addr[len('pkl://'):]
    elif options.addr.startswith('postgres://'):
        import analyze.pg
        dbstring = options.addr
        db = sql_db(dbstring)
        username, password, hostname, dbname, tablename = parse_dbstring(dbstring)
    elif options.addr.startswith('dd://'):
        raise NotImplementedError()
        import analyze.dd
    else:
        raise NotImplementedError('unknown address format, it should start with "pkl" or'
                ' "postgres" or "dd"', options.addr)

    # import modules named via --extra
    for extra in options.extra.split(','):
        if extra:
            _logger.debug('importing extra module: %s'% extra)
            __import__(extra)

    try:
        cmd = cmd_dct[cmdname]
    except:
        cmd = help
    cmd(**locals())

runner_registry['analyze'] = (parser_analyze, runner_analyze)

##############################
# Analyze sub-command registry
##############################

class Cmd(object):
    """ A callable object that attaches documentation strings to command functions.
    
    This class is a helper for the decorators `cmd` and `cmd_desc`.
    """
    def __init__(self, f, desc):
        self.f = f
        if desc is None:
            self.desc = 'No help available'
        else:
            self.desc = desc

    def __call__(self, *args, **kwargs):
        return self.f(*args, **kwargs)
cmd_dct = {}
def cmd(f):
    """Declare a function `f` as a `mydriver.main` command.

    The docstring of `f` is taken as the description of the command.
    """
    cmd_dct[f.__name__] = Cmd(f, f.__doc__)
    return f
def cmd_desc(desc):
    """Declare a function `f` as a `mydriver.main` command, and provide an explicit description to appear to the right of your command when running the 'help' command.
    """
    def deco(f):
        cmd_dct[f.__name__] = Cmd(f, desc)
        return f
    return deco

def help(**kwargs):
    """Print help for this program"""
    print "Usage: jobman analyze <cmd>"
    #TODO
    print "Commands available:"
    for name, cmd in cmd_dct.iteritems():
        print "%20s - %s"%(name, cmd.desc)

@cmd
def extra_help(**kwargs):
    """Print mini-tutorial about the --extra option to jobman analyze"""
    print """
The argument to --extra should be a python file that adds commands to jobman's analyze
command.

For example: 

-------------------------------------------------------------------------------

from jobman.analyze_runner import cmd

@cmd
def mycmd(**kwargs):
    '''Silly command, doesn't do much.'''

    print 'My command has access to the keyword args, and can do whatever it likes'
    print kwargs

-------------------------------------------------------------------------------

Put this in a file in the current working directory called 'foo.py' (for compatibility with
this documentation only... when you get the hang of things you can call this file whatever you
like.  It can be anywhere in the PYTHONPATH.)

Then run

    $ jobman analyze --extra=foo list

You should see your 'mycmd' listed. Now run

    $ jobman analyze --extra=foo mycmd
    $ jobman analyze --extra=foo mycmd  hello
    $ jobman analyze --extra=foo mycmd  hello hello=5

You will see how these arguments have been passed to your function, and you can write custom
functions in this way to analyze the results of your experiment.

For documentation on how to insert new jobs, iterate over jobs, etc. refer to the file(s) in
`jobman.expdir` corresponding to the type of experiment you have.

    """
