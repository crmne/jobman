from runner import runner_registry
from optparse import OptionParser
import sys, logging, time, os

parser_raw = OptionParser(usage = '%prog raw [options] <expr>', add_help_option=True)
parser_raw.add_option('--workdir', 
        action='store', dest = 'workdir', type = 'str', default = '',
        help = 'Call function in a subdirectory.'
        ' The %(cmdfn)s substring will be replaced with the name of the fn.'
        ' The %(timestamp)s substring will be replaced with the current time.')
parser_raw.add_option('--no-latest', 
        action='store_true', dest = 'nolatest', default = False,
        help = 'suppress creation of jobman.latest')
parser_raw.add_option('--log', 
        action='store_true', dest = 'log', default = False,
        help = 'enable logging module')
parser_raw.add_option('--logfile', 
        action='store', dest = 'logfile', type='str', default = '',
        help = 'append logging to LOGFILE. Implies --log')
def import_cmd(cmd):
    """Return the full module name of a fully-quallified function call
    """
    #print 'cmd', cmd
    lp = cmd.index('(')
    ftoks = cmd[:lp].split('.')
    imp = '.'.join(ftoks[:-1])
    return imp, cmd

_logger = logging.getLogger('jobman.raw_runner')
_logger.setLevel(logging.INFO)

def runner_raw(options, fullfn):
    """ Run a fully-qualified python function from the commandline.

    Example use:

        jobman raw 'mymodule.my_experiment(0, 1, a=2, b=3)'

    """
    imp, cmd = import_cmd(fullfn)
    cmdfn = cmd[:cmd.index('(')]
    timestamp = '.'.join('%02i'%s for s in time.localtime()[:6])
    if options.workdir:
        dirname = options.workdir % locals()
        os.makedirs(dirname)
        if not options.nolatest:
            try:
                os.remove('jobman.latest')
            except:
                pass
            os.system('ln -s "%s" jobman.latest' % dirname)
        os.chdir(dirname)

    if options.log or options.logfile:
        if options.logfile:
            logfilename = options.logfile
        else:
            logfilename = 'jobman_%(cmdfn)s_%(timestamp)s.log'
        logging.basicConfig(
                level=logging.DEBUG, 
                stream=open(logfilename%locals(), 'a+'))

    _logger.info('Running: %s' % cmd)
    _logger.info('Starttime: %s' % time.localtime())
    _logger.debug('Importing: %s' % imp)
    if imp:
        exec('import '+imp)
    t0 = time.time()
    _logger.debug('executing: %s' % cmd)
    exec(cmd)
    _logger.info('Endtime: %s' % time.localtime())
    _logger.info('Duration: %s', time.time() - t0)

runner_registry['raw'] = (parser_raw, runner_raw)

