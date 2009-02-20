from __future__ import with_statement

import sql
import os
import tempfile
import shutil
import socket
import optparse
from optparse import OptionParser

from tools import *
from runner import runner_registry
from channel import StandardChannel, JobError


################################################################################
### Channels
################################################################################

################################################################################
### RSync channel
################################################################################

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


################################################################################
### DB + RSync channel
################################################################################

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
            experiment = resolve(state.jobman.experiment)
            remote_path = os.path.join(remote_root, self.dbname, self.tablename, str(self.dbstate.id))
            super(DBRSyncChannel, self).__init__(path, remote_path, experiment, state, redirect_stdout, redirect_stderr)
        except:
            self.dbstate['jobman.status'] = self.DONE
            raise

    def save(self):
        super(DBRSyncChannel, self).save()
        self.dbstate.update(flatten(self.state))

    def setup(self):
        # Extract a single experiment from the table that is not already running.
        # set self.experiment and self.state
        super(DBRSyncChannel, self).setup()
        self.state.jobman.sql.host_name = socket.gethostname()
        self.state.jobman.sql.host_workdir = self.path
        self.dbstate.update(flatten(self.state))

    def run(self):
        # We pass the force flag as True because the status flag is
        # already set to RUNNING by book_dct in __init__
        v = super(DBRSyncChannel, self).run(force = True)
        if v is self.INCOMPLETE and self.state.jobman.sql.priority != self.RESTART_PRIORITY:
            self.state.jobman.sql.priority = self.RESTART_PRIORITY
            self.save()
        return v



################################################################################
### Runners
################################################################################

################################################################################
### sqlschedule
################################################################################

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
        jobman sqlschedule postgres://user:pass@host/dbname/tablename \\
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
    state['jobman.experiment'] = experiment
    sql.add_experiments_to_db([state], db, verbose = 1, add_dups = options.force)

runner_registry['sqlschedule'] = (parser_sqlschedule, runner_sqlschedule)


################################################################################
### sqlschedule_filemerge
################################################################################

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
    state['jobman.experiment'] = experiment
    sql.add_experiments_to_db([state], db, verbose = 1, add_dups = options.force)

runner_registry['sqlschedule_filemerge'] = (parser_sqlschedule_filemerge, runner_sqlschedule_filemerge)


################################################################################
### sql
################################################################################

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
        jobman sql \\
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
