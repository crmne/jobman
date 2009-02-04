
from __future__ import with_statement

from collections import defaultdict
import re, sys, inspect, os, signal, tempfile, shutil, socket
import traceback

import sql


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

################################################################################
### single channels
################################################################################

# try:
#     import greenlet
# except:
#     try:
#         from py import greenlet
#     except:
#         print >>sys.stderr, 'the greenlet module is unavailable'
#         greenlet = None


class Channel(object):

    COMPLETE = property(lambda s:None,
            doc=("Experiments should return this value to "
                "indicate that they are done (if not done, return `Incomplete`"))
    INCOMPLETE = property(lambda s:True,
            doc=("Experiments should return this value to indicate that "
            "they are not done (if done return `COMPLETE`)"))
    
    START = property(lambda s: 0,
            doc="dbdict.status == START means a experiment is ready to run")
    RUNNING = property(lambda s: 1,
            doc="dbdict.status == RUNNING means a experiment is running on dbdict_hostname")
    DONE = property(lambda s: 2,
            doc="dbdict.status == DONE means a experiment has completed (not necessarily successfully)")

    # Methods to be used by the experiment to communicate with the channel

    def save(self):
        """
        Save the experiment's state to the various media supported by
        the Channel.
        """
        raise NotImplementedError()

    def switch(self, message = None):
        """
        Called from the experiment to give the control back to the channel.
        The following return values are meaningful:
          * 'stop' -> the experiment must stop as soon as possible. It may save what
            it needs to save. This occurs when SIGTERM or SIGINT are sent (or in
            user-defined circumstances).
        switch() may give the control to the user. In this case, the user may
        resume the experiment by calling switch() again. If an argument is given
        by the user, it will be relayed to the experiment.
        """
        pass

    def __call__(self, message = None):
        return self.switch(message)

    def save_and_switch(self):
        self.save()
        self.switch()

    # Methods to run the experiment

    def setup(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self):
        pass

    def run(self):
        pass



class JobError(Exception):
    RUNNING = 0
    DONE = 1
    NOJOB = 2


class SingleChannel(Channel):

    def __init__(self, experiment, state):
        self.experiment = experiment
        self.state = state
        self.feedback = None

        #TODO: make this a property and disallow changing it during a with block
        self.catch_sigterm = True
        self.catch_sigint = True

    def switch(self, message = None):
        feedback = self.feedback
        self.feedback = None
        return feedback

    def run(self, force = False):
        self.setup()

        status = self.state.dbdict.get('status', self.START)
        if status is self.DONE and not force:
            # If you want to disregard this, use the --force flag (not yet implemented)
            raise JobError(JobError.RUNNING,
                           'The job has already completed.')
        elif status is self.RUNNING and not force:
            raise JobError(JobError.DONE,
                           'The job is already running.')
        self.state.dbdict.status = self.RUNNING

        v = self.COMPLETE
        with self: #calls __enter__ and then __exit__
            try:
                v = self.experiment(self.state, self)
            finally:
                self.state.dbdict.status = self.DONE if v is self.COMPLETE else self.START

        return v

    def on_sigterm(self, signo, frame):
        # SIGTERM handler. It is the experiment function's responsibility to
        # call switch() often enough to get this feedback.
        self.feedback = 'stop'

    def __enter__(self):
        # install a SIGTERM handler that asks the experiment function to return
        # the next time it will call switch()
        if self.catch_sigterm:
            self.prev_sigterm = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGTERM, self.on_sigterm)
        if self.catch_sigint:
            self.prev_sigint = signal.getsignal(signal.SIGINT)
            signal.signal(signal.SIGINT, self.on_sigterm)
        return self

    def __exit__(self, type, value, tb_traceback, save = True):
        if type:
            try:
                raise type, value, tb_traceback
            except:
                traceback.print_exc()
        if self.catch_sigterm:
            signal.signal(signal.SIGTERM, self.prev_sigterm)
            self.prev_sigterm = None
        if self.catch_sigint:
            signal.signal(signal.SIGINT, self.prev_sigint)
            self.prev_sigint = None
        if save:
            self.save()
        return True


class StandardChannel(SingleChannel):

    def __init__(self, path, experiment, state, redirect_stdout = False, redirect_stderr = False):
        super(StandardChannel, self).__init__(experiment, state)
        self.path = os.path.realpath(path)
        self.redirect_stdout = redirect_stdout
        self.redirect_stderr = redirect_stderr

    def save(self):
        with open(os.path.join(self.path, 'current.conf'), 'w') as current:
            current.write(format_d(self.state))
            current.write('\n')

    def __enter__(self):
        self.old_cwd = os.getcwd()
        os.chdir(self.path)
        if self.redirect_stdout:
            self.old_stdout = sys.stdout
            sys.stdout = open('stdout', 'a')
        if self.redirect_stderr:
            self.old_stderr = sys.stderr
            sys.stderr = open('stderr', 'a')
        return super(StandardChannel, self).__enter__()

    def __exit__(self, type, value, traceback):
        rval = super(StandardChannel, self).__exit__(type, value, traceback, save = False)
        if self.redirect_stdout:
            sys.stdout.close()
            sys.stdout = self.old_stdout
        if self.redirect_stderr:
            sys.stderr.close()
            sys.stderr = self.old_stderr
        os.chdir(self.old_cwd)
        self.save()
        return rval

    def setup(self):
        if not os.path.isdir(self.path):
            os.makedirs(self.path)
        with self:
            origf = os.path.join(self.path, 'orig.conf')
            if not os.path.isfile(origf):
                with open(origf, 'w') as orig:
                    orig.write(format_d(self.state))
                    orig.write('\n')
            currentf = os.path.join(self.path, 'current.conf')
            if os.path.isfile(currentf):
                with open(currentf, 'r') as current:
                    self.state = expand(parse(*map(str.strip, current.readlines())))


class RSyncException(Exception):
    pass

class RSyncChannel(StandardChannel):

    def __init__(self, path, remote_path, experiment, state, redirect_stdout = False, redirect_stderr = False):
        super(RSyncChannel, self).__init__(path, experiment, state, redirect_stdout, redirect_stderr)

        ssh_prefix='ssh://'
        if remote_path.startswith(ssh_prefix):
            remote_path = remote_path[len(ssh_prefix):]
            colon_pos = remote_path.find(':')
            self.host = remote_path[:colon_pos]
            self.remote_path = remote_path[colon_pos+1:]
        else:
            self.host = ''
            self.remote_path = os.path.realpath(remote_path)

    def rsync(self, direction):
        """The directory at which experiment-related files are stored.
        """

        path = self.path

        remote_path = self.remote_path
        if self.host:
            remote_path = ':'.join([self.host, remote_path])

        # TODO: use something more portable than os.system
        if direction == 'push':
            rsync_cmd = 'rsync -ac "%s/" "%s/"' % (path, remote_path)
        elif direction == 'pull':
            rsync_cmd = 'rsync -ac "%s/" "%s/"' % (remote_path, path)
        else:
            raise RSyncException('invalid direction', direction)

        rsync_rval = os.system(rsync_cmd)
        if rsync_rval != 0:
            raise RSyncException('rsync failure', (rsync_rval, rsync_cmd))

    def touch(self):
        if self.host:
            host = self.host
            touch_cmd = ('ssh %(host)s  "mkdir -p \'%(path)s\'"' % dict(host = self.host,
                                                                        path = self.remote_path))
        else:
            touch_cmd = ("mkdir -p '%(path)s'" % dict(path = self.remote_path))
        # print "ECHO", touch_cmd
        touch_rval = os.system(touch_cmd)
        if 0 != touch_rval:
            raise Exception('touch failure', (touch_rval, touch_cmd))

    def pull(self):
        return self.rsync('pull')

    def push(self):
        return self.rsync('push')

    def save(self):
        super(RSyncChannel, self).save()
        self.push()

    def setup(self):
        self.touch()
        self.pull()
        super(RSyncChannel, self).setup()


class DBRSyncChannel(RSyncChannel):

    RESTART_PRIORITY = 2.0

    def __init__(self, username, password, hostname, dbname, tablename, path, remote_root, redirect_stdout = False, redirect_stderr = False):
        self.username, self.password, self.hostname, self.dbname, self.tablename \
            = username, password, hostname, dbname, tablename

        self.db = sql.postgres_serial(
            user = self.username, 
            password = self.password, 
            host = self.hostname,
            database = self.dbname,
            table_prefix = self.tablename)

        self.dbstate = sql.book_dct_postgres_serial(self.db)
        if self.dbstate is None:
            raise JobError(JobError.NOJOB,
                           'No job was found to run.')

        try:
            state = expand(self.dbstate)
            experiment = resolve(state.dbdict.experiment)
            remote_path = os.path.join(remote_root, self.dbname, self.tablename, str(self.dbstate.id))
            super(DBRSyncChannel, self).__init__(path, remote_path, experiment, state, redirect_stdout, redirect_stderr)
        except:
            self.dbstate['dbdict.status'] = self.DONE
            raise

    def save(self):
        super(DBRSyncChannel, self).save()
        self.dbstate.update(flatten(self.state))
    
    def setup(self):
        # Extract a single experiment from the table that is not already running.
        # set self.experiment and self.state
        super(DBRSyncChannel, self).setup()
        self.state.dbdict.sql.host_name = socket.gethostname()
        self.state.dbdict.sql.host_workdir = self.path
        self.dbstate.update(flatten(self.state))

    def run(self):
        # We pass the force flag as True because the status flag is
        # already set to RUNNING by book_dct in __init__
        v = super(DBRSyncChannel, self).run(force = True)
        if v is self.INCOMPLETE and self.state.dbdict.sql.priority != self.RESTART_PRIORITY:
            self.state.dbdict.sql.priority = self.RESTART_PRIORITY
            self.save()
        return v



################################################################################
### running
################################################################################

import optparse
OptionParser = optparse.OptionParser
# class OptionParser(optparse.OptionParser):
#     def error(self, message):
#         pass

def parse_and_run(command, arguments):
    parser, runner = runner_registry.get(command, (None, None))
    if not runner:
        raise UsageError('Unknown runner: "%s"' % command)
    if parser:
        options, arguments = parser.parse_args(arguments)
    else:
        options = optparse.Values()
    run(runner, [options] + arguments)

def run(runner, arguments):
    argspec = inspect.getargspec(runner)
    minargs = len(argspec[0])-(len(argspec[3]) if argspec[3] else 0)
    maxargs = len(argspec[0])
    if minargs > len(arguments) or maxargs < len(arguments) and not argspec[1]:
        s = format_help(runner)
        raise UsageError(s)
    runner(*arguments)

runner_registry = dict()


parser_cmdline = OptionParser(usage = '%prog cmdline [options] <experiment> <parameters>')
parser_cmdline.add_option('-f', '--force', action = 'store_true', dest = 'force', default = False,
                          help = 'force running the experiment even if it is already running or completed')
parser_cmdline.add_option('--redirect-stdout', action = 'store_true', dest = 'redirect_stdout', default = False,
                          help = 'redirect stdout to the workdir/stdout file')
parser_cmdline.add_option('--redirect-stderr', action = 'store_true', dest = 'redirect_stderr', default = False,
                          help = 'redirect stderr to the workdir/stdout file')
parser_cmdline.add_option('-r', '--redirect', action = 'store_true', dest = 'redirect', default = False,
                          help = 'redirect stdout and stderr to the workdir/stdout and workdir/stderr files')
parser_cmdline.add_option('-w', '--workdir', action = 'store', dest = 'workdir', default = None,
                          help = 'the working directory in which to run the experiment')
parser_cmdline.add_option('-n', '--dry-run', action = 'store_true', dest = 'dry_run', default = False,
                          help = 'use this option to run the whole experiment in a temporary working directory (cleaned after use)')
parser_cmdline.add_option('-2', '--sigint', action = 'store_true', dest = 'allow_sigint', default = False,
        help = 'allow sigint (CTRL-C) to interrupt a process')

def runner_cmdline(options, experiment, *strings):
    """
    Start an experiment with parameters given on the command line.

    Usage: cmdline [options] <experiment> <parameters>

    Run an experiment with parameters provided on the command
    line. See the help topics for experiment and parameters for
    syntax information.

    Example use:
        dbdict-run cmdline mymodule.my_experiment \\
            stopper::pylearn.stopper.nsteps \\ # use pylearn.stopper.nsteps
            stopper.n=10000 \\ # the argument "n" of nsteps is 10000
            lr=0.03
    """
    state = expand(parse(*strings))
    state.setdefault('dbdict', DD()).experiment = experiment
    experiment = resolve(experiment)
    if options.workdir and options.dry_run:
        raise UsageError('Please use only one of: --workdir, --dry-run.')
    if options.workdir:
        workdir = options.workdir
    elif options.dry_run:
        workdir = tempfile.mkdtemp()
    else:
        workdir = format_d(state, sep=',', space = False)
    channel = StandardChannel(workdir,
                              experiment, state,
                              redirect_stdout = options.redirect or options.redirect_stdout,
                              redirect_stderr = options.redirect or options.redirect_stderr)
    channel.catch_sigint = not options.allow_sigint
    channel.run(force = options.force)
    if options.dry_run:
        shutil.rmtree(workdir, ignore_errors=True)
        
runner_registry['cmdline'] = (parser_cmdline, runner_cmdline)




parser_filemerge = OptionParser(usage = '%prog filemerge [options] <experiment> <file> <file2> ...')
parser_filemerge.add_option('-f', '--force', action = 'store_true', dest = 'force', default = False,
                          help = 'force running the experiment even if it is already running or completed')
parser_filemerge.add_option('--redirect-stdout', action = 'store_true', dest = 'redirect_stdout', default = False,
                          help = 'redirect stdout to the workdir/stdout file')
parser_filemerge.add_option('--redirect-stderr', action = 'store_true', dest = 'redirect_stderr', default = False,
                          help = 'redirect stderr to the workdir/stdout file')
parser_filemerge.add_option('-r', '--redirect', action = 'store_true', dest = 'redirect', default = False,
                          help = 'redirect stdout and stderr to the workdir/stdout and workdir/stderr files')
parser_filemerge.add_option('-w', '--workdir', action = 'store', dest = 'workdir', default = None,
                          help = 'the working directory in which to run the experiment')
parser_filemerge.add_option('-n', '--dry-run', action = 'store_true', dest = 'dry_run', default = False,
                          help = 'use this option to run the whole experiment in a temporary working directory (cleaned after use)')

def runner_filemerge(options, experiment, mainfile, *other_files):
    """
    Start an experiment with parameters given in files.

    Usage: filemerge [options] <experiment> <file> <file2> ...

    Run an experiment with parameters provided in plain text files.
    A single experiment will be run with the union of all the
    parameters listed in the files.

    Example:
    <in file blah1.txt>
    text.first = "hello"
    text.second = "world"

    <in file blah2.txt>
    number = 12
    numbers.a = 55
    numbers.b = 56

    Given these files, the following command using filemerge:
    $ dbdict-run filemerge mymodule.my_experiment blah1.txt blah2.txt

    is equivalent to this one using cmdline:
    $ dbdict-run cmdline mymodule.my_experiment \\
        text.first=hello text.second=world \\
        number=12 numbers.a=55 numbers.b=56
    """
    with open(mainfile) as f:
        _state = parse(*map(str.strip, f.readlines()))
    for file in other_files:
        if '=' in file:
            _state.update(parse(file))
        else:
            with open(file) as f:
                _state.update(parse(*map(str.strip, f.readlines())))
    state = expand(_state)
    state.setdefault('dbdict', DD()).experiment = experiment
    experiment = resolve(experiment)
    if options.workdir and options.dry_run:
        raise UsageError('Please use only one of: --workdir, --dry-run.')
    if options.workdir:
        workdir = options.workdir
    elif options.dry_run:
        workdir = tempfile.mkdtemp()
    else:
        workdir = format_d(state, sep=',', space = False)
    channel = StandardChannel(workdir,
                              experiment, state,
                              redirect_stdout = options.redirect or options.redirect_stdout,
                              redirect_stderr = options.redirect or options.redirect_stderr)
    channel.run(force = options.force)
    if options.dry_run:
        shutil.rmtree(workdir, ignore_errors=True)
        
runner_registry['filemerge'] = (parser_filemerge, runner_filemerge)




parser_sqlschedule = OptionParser(usage = '%prog sqlschedule [options] <tablepath> <experiment> <parameters>')
parser_sqlschedule.add_option('-f', '--force', action = 'store_true', dest = 'force', default = False,
                              help = 'force adding the experiment to the database even if it is already there')

def runner_sqlschedule(options, dbdescr, experiment, *strings):
    """
    Schedule a job to run using the sql command.

    Usage: sqlschedule <tablepath> <experiment> <parameters>

    See the experiment and parameters topics for more information about
    these parameters.

    Assuming that a postgres database is running on `host`, contains a
    database called `dbname` and that `user` has the permissions to
    create, read and modify tables on that database, tablepath should
    be of the following form:
        postgres://user:pass@host/dbname/tablename

    If no table is named `tablename`, one will be created
    automatically. The state corresponding to the experiment and
    parameters specified in the command will be saved in the database,
    but no experiment will be run.

    To run an experiment scheduled using sqlschedule, see the sql
    command.

    Example use:
        dbdict-run sqlschedule postgres://user:pass@host/dbname/tablename \\
            mymodule.my_experiment \\
            stopper::pylearn.stopper.nsteps \\ # use pylearn.stopper.nsteps
            stopper.n=10000 \\ # the argument "n" of nsteps is 10000
            lr=0.03
    """

    try:
        username, password, hostname, dbname, tablename \
            = sql.parse_dbstring(dbdescr)
    except:
        raise UsageError('Wrong syntax for dbdescr')

    db = sql.postgres_serial(
        user = username, 
        password = password, 
        host = hostname,
        database = dbname,
        table_prefix = tablename)

    state = parse(*strings)
    resolve(experiment) # we try to load the function associated to the experiment
    state['dbdict.experiment'] = experiment
    sql.add_experiments_to_db([state], db, verbose = 1, add_dups = options.force)

runner_registry['sqlschedule'] = (parser_sqlschedule, runner_sqlschedule)



parser_sqlschedule_filemerge = OptionParser(usage = '%prog sqlschedule_filemerge [options] <tablepath> <experiment> <parameters|files>')
parser_sqlschedule_filemerge.add_option('-f', '--force', action = 'store_true', dest = 'force', default = False,
                                        help = 'force adding the experiment to the database even if it is already there')

def runner_sqlschedule_filemerge(options, dbdescr, experiment, mainfile, *other_files):
    """
    Schedule a job to run using the sql command using parameter files.

    This command is to sqlschedule what the filemerge command is to
    cmdline.
    """

    try:
        username, password, hostname, dbname, tablename \
            = sql.parse_dbstring(dbdescr)
    except:
        raise UsageError('Wrong syntax for dbdescr')

    db = sql.postgres_serial(
        user = username, 
        password = password, 
        host = hostname,
        database = dbname,
        table_prefix = tablename)

    with open(mainfile) as f:
        _state = parse(*map(str.strip, f.readlines()))
    for file in other_files:
        if '=' in file:
            _state.update(parse(file))
        else:
            with open(file) as f:
                _state.update(parse(*map(str.strip, f.readlines())))
    state = _state

    resolve(experiment) # we try to load the function associated to the experiment
    state['dbdict.experiment'] = experiment
    sql.add_experiments_to_db([state], db, verbose = 1, add_dups = options.force)

runner_registry['sqlschedule_filemerge'] = (parser_sqlschedule_filemerge, runner_sqlschedule_filemerge)




parser_sql = OptionParser(usage = '%prog sql [options] <tablepath> <exproot>')
parser_sql.add_option('-n', dest = 'n', type = 'int', default = 1,
                      help = 'Run N experiments sequentially (default 1) '
                      '(if N is <= 0, runs as many experiments as possible).')

def runner_sql(options, dbdescr, exproot):
    """
    Run jobs from a sql table.

    Usage: sql <tablepath> <exproot>

    The jobs should be scheduled first with the sqlschedule command.

    Assuming that a postgres database is running on `host`, contains a
    database called `dbname` and that `user` has the permissions to
    create, read and modify tables on that database, tablepath should
    be of the following form:
        postgres://user:pass@host/dbname/tablename

    exproot can be a local path or a remote path. Examples of exproots:
        /some/local/path
        ssh://some_host:/some/remote/path # relative to the filesystem root
        ssh://some_host:other/remote/path # relative to the HOME on some_host

    The exproot will contain a subdirectory hierarchy corresponding to
    the dbname, tablename and job id which is a unique integer.

    The sql runner will pick any job in the table which is not running
    and is not done and will terminate when that job ends. You may call
    the same command multiple times, sequentially or in parallel, to
    run as many unfinished jobs as have been scheduled in that table
    with sqlschedule.

    Example use:
        dbdict-run sql \\
            postgres://user:pass@host/dbname/tablename \\
            ssh://central_host:myexperiments
    """
    try:
        username, password, hostname, dbname, tablename \
            = sql.parse_dbstring(dbdescr)
    except:
        raise UsageError('Wrong syntax for dbdescr')

    n = options.n if options.n else -1
    nrun = 0
    try:
        while n != 0:
            workdir = tempfile.mkdtemp()
            #print 'wdir', workdir
            channel = DBRSyncChannel(username, password, hostname, dbname, tablename,
                                     workdir,
                                     exproot,
                                     redirect_stdout = True,
                                     redirect_stderr = True)
            channel.run()
            shutil.rmtree(workdir, ignore_errors=True)
            n -= 1
            nrun += 1
    except JobError, e:
        if e.args[0] == JobError.NOJOB:
            print 'No more jobs to run (run %i jobs)' % nrun

runner_registry['sql'] = (parser_sql, runner_sql)

    



def runner_help(options, topic = None):
    """
    Get help for a topic.

    Usage: help <topic>
    """
    def bold(x):
        return '\033[1m%s\033[0m' % x
    if topic is None:
        print bold('Topics: (use help <topic> for more info)')
        print 'example        Example of defining and running an experiment.'
        print 'experiment     How to define an experiment.'
        print 'parameters     How to list the parameters for an experiment.'
        print
        print bold('Available commands: (use help <command> for more info)')
        for name, (parser, command) in sorted(runner_registry.iteritems()):
            print name.ljust(20), format_help(command).split('\n')[0]
        return
    elif topic == 'experiment':
        helptext = """

        dbdict-run serves to run experiments. To define an experiment, you
        only have to define a function respecting the following protocol in
        a python file or module:

        def my_experiment(state, channel):
           # experiment code goes here

        The return value of my_experiment may be channel.COMPLETE or
        channel.INCOMPLETE. If the latter is returned, the experiment may
        be resumed at a later point. Note that the return value `None`
        is interpreted as channel.COMPLETE.

        If a command defined by dbdict-run has an <experiment> parameter,
        that parameter must be a string such that it could be used in a
        python import statement to import the my_experiment function. For
        example if you defined my_experiment in my_module.py, you can pass
        'my_module.my_experiment' as the experiment parameter.

        When entering my_experiment, the current working directory will be
        set for you to a directory specially created for the experiment.
        The location and name of that directory vary depending on which
        dbdict-run command you run. You may create logs, save files, pictures,
        results, etc. in it.

        state is an object containing the parameters given to the experiment.
        For example, if you run the followinc command:

        dbdict-run cmdline my_module.my_experiment a.x=6

        `state.a.x` will contain the integer 6, and so will `state['a']['x']`.
        If the state is changed, it will be saved when the experiment ends
        or when channel.save() is called. The next time the experiment is run
        with the same working directory, the modified state will be provided.

        It is not recommended to store large amounts of data in the state.  It
        should be limited to scalar or string parameters. Results such as
        weight matrices should be stored in files in the working directory.

        channel is an object with the following important methods:

         - channel.switch() (or channel()) will give the control back to the
            user, if it is appropriate to do so. If a call to channel.switch()
            returns the string 'stop', it typically means that the signal
            SIGTERM (or SIGINT) was received. Therefore, the experiment may be
            killed soon, so it should save and return True or
            channel.INCOMPLETE so it can be resumed later. This should be
            checked periodically or data loss may be incurred.

         - channel.save() will save the current state. It is automatically
            called when the function returns, but it is a good idea to do it
            periodically.

         - channel.save_and_switch() is an useful shortcut to do both operations
            described above.
        """

    elif topic == 'parameters':
        helptext = """
        If a command takes <parameters> arguments, the arguments should each
        take one of the following forms:

        key=value

          Set a parameter with name `key` to `value`. The value will be casted
          to an appropriate type automatically and it will be accessible to
          the experiment using `state.key`.

          If `key` is a dotted name, the value will be set in nested
          dictionaries corresponding to each part.

          Examples:
            a=1           state.a <- 1
            b=2.3         state.b <- 2.3
            c.d="hello"   state.c.d <- "hello"

        key::builder

          This is equivalent to key.__builder__=builder.

          The builder should be a symbol that can be used with import or
          __import__ and it should be callable.

          If a key has a builder defined, the experiment code may easily make
          an object out of it using the `make` function. `obj = make(state.key)`.
          This will call the builder on the substate corresponding to state.key,
          as will be made clear in the example:

          Example:
            regexp::re.compile
            regexp.pattern='a.*c'

          from pylearn.dbdict.newstuff import make
          def experiment(state, channel):
              regexp = make(state.regexp) # regexp is now re.compile(pattern = 'a.*c')
              print regexp.sub('blahblah', 'hello abbbbc there')

          If the above experiment was called with the state produced by the
          parameters in the example, it would print 'hello blahblah there'.
        """

    elif topic == 'example':
        helptext = """
        Example of an experiment that trains some model for 100000 iterations:

        # defined in: my_experiments.py
        def experiment(state, channel):
            try:
                model = cPickle.load(open('model', 'r'))
            except:
                model = my_model(state.some_param, state.other_param)
                state.n = 0
            dataset = my_dataset(skipto = state.n)
            for i in xrange(100000 - state.n):
                model.update(dataset.next())
                if i and i % 1000 == 0:
                    if channel.save_and_switch() == 'stop':
                        state.n += i + 1
                        rval = channel.INCOMPLETE
                        break
            else:
                state.result = model.cost(some_test_set)
                rval = channel.COMPLETE
            cPickle.dump(model, open('model', 'w'))
            return rval

        And then you could run it this way:
        
        dbdict-run cmdline my_experiments.experiment \\
                           some_param=1 \\
                           other_param=0.4

        Or this way:

        dbdict-run sqlschedule postgres://user:pass@host/dbname/tablename \\
                           my_experiments.experiment \\
                           some_param=1 \\
                           other_param=0.4

        dbdict-run sql postgres://user:pass@host/dbname/tablename exproot

        You need to make sure that the module `my_experiments` is accessible
        from python. You can check with the command

        $ python -m my_experiments
        """
    else:
        helptext = runner_registry.get(topic, None)[1]
    print format_help(helptext)

runner_registry['help'] = (None, runner_help)

################################################################################
### main
################################################################################

def run_cmdline():
    try:
        if len(sys.argv) <= 1:
            raise UsageError('Usage: %s <run_type> [<arguments>*]' % sys.argv[0])
        cmd = None
        args = []
        for arg in sys.argv[1:]:
            if cmd is not None or arg.startswith('-'):
                args.append(arg)
            else:
                cmd = arg
        parse_and_run(cmd, args)
    except UsageError, e:
        print 'Usage error:'
        print e

if __name__ == '__main__':
    run_cmdline()


