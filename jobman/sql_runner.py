""" WRITEME """
from __future__ import with_statement

try:
    import sql
    from cachesync_runner import cachesync_lock
    import cachesync_runner
except:
    pass
import os
import tempfile
import shutil
import socket
import optparse
import time
import random
from optparse import OptionParser

from tools import *
from runner import runner_registry
from channel import StandardChannel, JobError
import parse


################################################################################
### Channels
################################################################################

################################################################################
### RSync channel
################################################################################

class RSyncException(Exception):
    pass

class RSyncChannel(StandardChannel):
    """ WRITEME """

    def __init__(self, path, remote_path, experiment, state,
            redirect_stdout = False, redirect_stderr = False,
            finish_up_after = None, save_interval = None):
        super(RSyncChannel, self).__init__(path, experiment, state, redirect_stdout, redirect_stderr,
                finish_up_after, save_interval)

        ssh_prefix='ssh://'
        if remote_path.startswith(ssh_prefix):
            remote_path = remote_path[len(ssh_prefix):]
            colon_pos = remote_path.find(':')
            self.host = remote_path[:colon_pos]
            self.remote_path = remote_path[colon_pos+1:]
        else:
            self.host = ''
            self.remote_path = os.path.realpath(remote_path)

        # If False, do not rsync during save.
        # This is useful if we have to halt with short notice.
        self.sync_in_save = True

    def rsync(self, direction, num_retries=3):
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

        keep_trying = True
        rsync_rval = 1 # some non-null value

        with cachesync_lock(None, self.path):
            # Useful for manual tests; leave this there, just commented.
            #cachesync_runner.manualtest_will_save()

            # allow n-number of retries, with random hold-off between retries
            attempt = 0
            while rsync_rval!=0 and keep_trying:
                rsync_rval = os.system(rsync_cmd)

                if rsync_rval != 0:
                    attempt += 1
                    keep_trying = attempt < num_retries
                    # wait anywhere from 30s to [2,4,6] mins before retrying
                    if keep_trying: 
                        r = random.randint(30,attempt*120)
                        print >> os.sys.stderr, ('RSync Error at attempt %i/%i:'\
                                +' sleeping %is') %(attempt,num_retries,r)
                        time.sleep(r)

        if rsync_rval!=0:
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
        # Useful for manual tests; leave this there, just commented.
        #cachesync_runner.manualtest_inc_save_count()

        if self.sync_in_save:
            super(RSyncChannel, self).save()
            self.push()
        #TODO: else: update current.conf with only state.jobman, push current.conf

    def setup(self):
        self.touch()
        self.pull()
        super(RSyncChannel, self).setup()


################################################################################
### DB + RSync channel
################################################################################

class DBRSyncChannel(RSyncChannel):
    """ WRITEME """

    RESTART_PRIORITY = 2.0

    def __init__(self, username, password, hostname, port, dbname, tablename, path, remote_root,
            redirect_stdout = False, redirect_stderr = False,
            finish_up_after = None, save_interval = None):
        self.username, self.password, self.hostname, self.port, self.dbname, self.tablename \
            = username, password, hostname, port, dbname, tablename

        self.db = sql.postgres_serial(
            user = self.username,
            password = self.password,
            host = self.hostname,
            port = self.port,
            database = self.dbname,
            table_prefix = self.tablename)

        self.dbstate = sql.book_dct_postgres_serial(self.db)
        if self.dbstate is None:
            raise JobError(JobError.NOJOB,
                           'No job was found to run.')

        print "Selected job id=%d in table=%s in db=%s on host=%s:%i" % (self.dbstate.id,self.tablename, self.dbname, self.hostname, self.port)

        try:
            state = expand(self.dbstate)
            if state.has_key("dbdict"):
                state.jobman=state.dbdict
            experiment = resolve(state.jobman.experiment)
            remote_path = os.path.join(remote_root, self.dbname, self.tablename, str(self.dbstate.id))
            super(DBRSyncChannel, self).__init__(path, remote_path, experiment, state,
                    redirect_stdout, redirect_stderr, finish_up_after, save_interval)
        except:
            self.dbstate['jobman.status'] = self.ERR_START
            raise


    def save(self):
        # If the DB is not writable, the rsync won't happen
        # If the DB is up, but rsync fails, the status will be ERR_SYNC,
        # but self.state will not be updated in the database.
        session = self.db.session()
        try:
            # Test write access to DB
            self.dbstate.update_in_session({'jobman.status':self.ERR_SYNC}, session)

            # save self.state in file current.state, and rsync
            super(DBRSyncChannel, self).save()

            if self.sync_in_save:
                # update DB
                self.dbstate.update_in_session(flatten(self.state), session)
            else:
                # update only jobman.*
                state_jobman = flatten({'jobman':self.state.jobman})
                self.dbstate.update_in_session(state_jobman, session)
        finally:
            session.close()

    def setup(self):
        # Extract a single experiment from the table that is not already running.
        # set self.experiment and self.state
        super(DBRSyncChannel, self).setup()
        self.state.jobman.sql.host_name = socket.gethostname()
        self.state.jobman.sql.condor_slot = os.getenv("_CONDOR_SLOT","no_condor_slot")
        self.state.jobman.sql.start_time = time.time()
        self.state.jobman.sql.host_workdir = self.path
        self.dbstate.update(flatten(self.state))

    def touch(self):
        try:
            super(DBRSyncChannel, self).touch()
        except:
            self.dbstate['jobman.status'] = self.ERR_START
            raise

    def run(self):
        # We pass the force flag as True because the status flag is
        # already set to RUNNING by book_dct in __init__
        v = super(DBRSyncChannel, self).run(force = True)
        if v is self.INCOMPLETE and self.state.jobman.sql.priority < self.RESTART_PRIORITY:
            self.state.jobman.sql.priority = self.RESTART_PRIORITY
            self.save()
        return v



################################################################################
### Runners
################################################################################

################################################################################
### sqlschedule
################################################################################

parser_sqlschedule = OptionParser(
    usage = '%prog sqlschedule [options] <tablepath> <experiment> <parameters>',
    add_help_option=False)
parser_sqlschedule.add_option('-f', '--force', action = 'store_true', dest = 'force', default = False,
                              help = 'force adding the experiment to the database even if it is already there')
parser_sqlschedule.add_option('-p', '--parser', action = 'store', dest = 'parser', default = 'filemerge',
                              help = 'parser to use for the argument list provided on the command line (takes a list of strings, returns a state)')

def runner_sqlschedule(options, dbdescr, experiment, *strings):
    """
    Schedule a job to run using the sql command.

    Usage: sqlschedule <tablepath> <experiment> <parameters>

    See the experiment and parameters topics for more information about
    these parameters.

    Assuming that a postgres database is running on port `port` of
    `host`, contains a database called `dbname` and that `user` has the
    permissions to create, read and modify tables on that database,
    tablepath should be of the following form:

        postgres://user:pass@host[:port]/dbname/tablename

    If no table is named `tablename`, one will be created
    automatically. The state corresponding to the experiment and
    parameters specified in the command will be saved in the database,
    but no experiment will be run.

    To run an experiment scheduled using sqlschedule, see the sql
    command.

    Example use:
        jobman sqlschedule postgres://user:pass@host[:port]/dbname/tablename \\
            mymodule.my_experiment \\
            stopper::pylearn.stopper.nsteps \\ # use pylearn.stopper.nsteps
            stopper.n=10000 \\ # the argument "n" of nsteps is 10000
            lr=0.03

        you can use the jobman.experiments.example1 as a working 
        mymodule.my_experiment
    """

    try:
        username, password, hostname, port, dbname, tablename \
            = sql.parse_dbstring(dbdescr)
    except Exception, e:
        raise UsageError('Wrong syntax for dbdescr',e)
    db = sql.postgres_serial(
        user = username,
        password = password,
        host = hostname,
        port = port,
        database = dbname,
        table_prefix = tablename)

    parser = getattr(parse, options.parser, None) or resolve(options.parser)

    state = parser(*strings)
    resolve(experiment) # we try to load the function associated to the experiment
    state['jobman.experiment'] = experiment
    sql.add_experiments_to_db([state], db, verbose = 1, force_dup = options.force)

runner_registry['sqlschedule'] = (parser_sqlschedule, runner_sqlschedule)

################################################################################
### sqlschedules
################################################################################

parser_sqlschedules = OptionParser(
    usage = '%prog sqlschedule [options] <tablepath> <experiment> <parameters>',
    add_help_option=False)
parser_sqlschedules.add_option('-f', '--force', action = 'store_true', dest = 'force', default = False,
                              help = 'force adding the experiment to the database even if it is already there')
parser_sqlschedules.add_option('-r', '--repeat',
                               dest = 'repeat', default = 1, type='int',
                               help = 'repeat each jobs N times')
parser_sqlschedules.add_option('-p', '--parser', action = 'store', dest = 'parser', default = 'filemerge',
                               help = 'parser to use for the argument list provided on the command line (takes a list of strings, returns a state)')

def generate_combination(repl):
    if repl == []:
        return []
    else:
        res = []
        x = repl[0]
        res1 = generate_combination(repl[1:])
        for y in x:
            if res1 == []:
                res.append([y])
            else:
                res.extend([[y]+r for r in res1])
        return res

def generate_commands(sp):
### Find replacement lists in the arguments
    repl = []
    p = re.compile('\{\{\S*?\}\}')
    for arg in sp:
        reg = p.findall(arg)
        if len(reg)==1:
            reg = p.search(arg)
            curargs = reg.group()[2:-2].split(",")
            newcurargs = []
            for curarg in curargs:
                new = p.sub(curarg,arg)
                newcurargs.append(new)
            repl.append(newcurargs)
        elif len(reg)>1:
            s=p.split(arg)
            tmp=[]
            for i in range(len(reg)):
                if s[i]:
                    tmp.append(s[i])
                tmp.append(reg[i][2:-2].split(","))
            i+=1
            if s[i]:
                tmp.append(s[i])
            repl.append(generate_combination(tmp,''))
        else:
            repl.append([arg])
    argscombination = generate_combination(repl)
    args_modif = generate_combination([x for x in repl if len(x)>1])

    return (argscombination,args_modif)

def runner_sqlschedules(options, dbdescr, experiment, *strings):
    """
    Schedule multiple jobs from the command line to run using the sql command.

    Usage: sqlschedules <tablepath> <experiment> <parameters>

    See the sqlschedule command for <tablepath> <experiment>
    We accept the dbidispatch syntax:
    where <parameters> is interpreted as follows:

      The parameters may contain one or many segments of the form
      {{a,b,c,d}}, which generate multiple jobs to execute. Each
      segement will be replaced by one value in the segment separated
      by comma. The first will have the a value, the second the b
      value, etc. If their is many segment, it will generate the
      cross-product of possible value between the segment.
    """

    try:
        username, password, hostname, port, dbname, tablename \
            = sql.parse_dbstring(dbdescr)
    except Exception, e:
        raise UsageError('Wrong syntax for dbdescr', e)

    parser = getattr(parse, options.parser, None) or resolve(options.parser)

    db = sql.postgres_serial(
        user = username,
        password = password,
        host = hostname,
        port = port,
        database = dbname,
        table_prefix = tablename)

    ### resolve(experiment) # we try to load the function associated to the experiment

    (commands,choise_args)=generate_commands(strings)
    print commands, choise_args

    if options.force:
        for cmd in commands:
            state = parser(*cmd)
            state['jobman.experiment'] = experiment
            sql.add_experiments_to_db([state]*(options.repeat), 
                                      db, verbose = 1, force_dup = True)
    else:
        #if the first insert fail, we won't force the other as the force option was not gived.
        failed = 0
        for cmd in commands:
            state = parser(*cmd)
            state['jobman.experiment'] = experiment
            ret = sql.add_experiments_to_db([state], db, verbose = 1, force_dup = options.force)
            if ret[0][0]:
                sql.add_experiments_to_db([state]*(options.repeat-1), db, 
                                          verbose = 1, force_dup = True)
            else:
                failed+=1
                print "The last cmd failed to insert, we won't repeat it. use --force to force the duplicate of job in the db."
        print "Added",len(commands)-failed,"on",len(commands)
runner_registry['sqlschedules'] = (parser_sqlschedules, runner_sqlschedules)

# ################################################################################
# ### sqlschedule_filemerge
# ################################################################################

# parser_sqlschedule_filemerge = OptionParser(
#     usage = '%prog sqlschedule_filemerge [options] <tablepath> <experiment> <parameters|files>',
#     add_help_option=False)
# parser_sqlschedule_filemerge.add_option('-f', '--force', action = 'store_true', dest = 'force', default = False,
#                                         help = 'force adding the experiment to the database even if it is already there')

# def runner_sqlschedule_filemerge(options, dbdescr, experiment, *files):
#     """
#     Schedule a job to run using the sql command using parameter files.

#     This command is to sqlschedule what the filemerge command is to
#     cmdline.
#     """

#     try:
#         username, password, hostname, port, dbname, tablename \
#             = sql.parse_dbstring(dbdescr)
#     except Exception, e:
#         raise UsageError('Wrong syntax for dbdescr',e)

#     db = sql.postgres_serial(
#         user = username,
#         password = password,
#         host = hostname,
#         port = port,
#         database = dbname,
#         table_prefix = tablename)

#     _state = parse_files(*files)

# #     with open(mainfile) as f:
# #         _state = parse(*map(str.strip, f.readlines()))
# #     for file in other_files:
# #         if '=' in file:
# #             _state.update(parse(file))
# #         else:
# #             with open(file) as f:
# #                 _state.update(parse(*map(str.strip, f.readlines())))

#     state = _state

#     resolve(experiment) # we try to load the function associated to the experiment
#     state['jobman.experiment'] = experiment
#     sql.add_experiments_to_db([state], db, verbose = 1, force_dup = options.force)

# runner_registry['sqlschedule_filemerge'] = (parser_sqlschedule_filemerge, runner_sqlschedule_filemerge)


################################################################################
### sql
################################################################################

parser_sql = OptionParser(usage = '%prog sql [options] <tablepath> <exproot>',
                          add_help_option=False)
parser_sql.add_option('-n', dest = 'n', type = 'int', default = 1,
                      help = 'Run N experiments sequentially (default 1) '
                      '(if N is <= 0, runs as many experiments as possible).')
parser_sql.add_option('--finish-up-after', action = 'store', dest = 'finish_up_after',
                          default = None,
                          help = 'Duration (in seconds) after which the experiment will be told to "finish up", i.e., to reach the next checkpoint, save, and exit')
parser_sql.add_option('--save-every', action = 'store', dest = 'save_every',
                          default = None,
                          help = 'Interval (in seconds) between checkpoints. --save-every=3600 will tell the experiment to reach the next checkpoint and save (and go on) every hour')

def runner_sql(options, dbdescr, exproot):
    """
    Run jobs from a sql table.

    Usage: sql [options] <tablepath> <exproot>

    The jobs should be scheduled first with the sqlschedule command.

    Assuming that a postgres database is running on port `port` of
    `host`, contains a database called `dbname` and that `user` has the
    permissions to create, read and modify tables on that database,
    tablepath should be of the following form:

        postgres://user:pass@host[:port]/dbname/tablename

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
            postgres://user:pass@host[:port]/dbname/tablename \\
            ssh://central_host:myexperiments
    """
    try:
        username, password, hostname, port, dbname, tablename \
            = sql.parse_dbstring(dbdescr)
    except Exception, e:
        raise UsageError('Wrong syntax for dbdescr',e)

    n = options.n if options.n else -1
    nrun = 0
    try:
        while n != 0:
            workdir = tempfile.mkdtemp()
            #print 'wdir', workdir
            channel = DBRSyncChannel(username, password, hostname, port, dbname,
                                     tablename,
                                     workdir,
                                     exproot,
                                     redirect_stdout = True,
                                     redirect_stderr = True,
                                     finish_up_after = options.finish_up_after or None,
                                     save_interval = options.save_every or None
                                     )
            channel.run()


            # Useful for manual tests; leave this there, just commented.
            #cachesync_runner.manualtest_before_delete()

            with cachesync_lock(None, workdir):
                # Useful for manual tests; leave this there, just commented.
                #cachesync_runner.manualtest_will_delete()

                shutil.rmtree(workdir, ignore_errors=True)

            n -= 1
            nrun += 1
    except JobError, e:
        if e.args[0] == JobError.NOJOB:
            print 'No more jobs to run (run %i jobs)' % nrun

runner_registry['sql'] = (parser_sql, runner_sql)

parser_sqlview = OptionParser(usage = '%prog sqlview <tablepath> <viewname>',
                              add_help_option=False)
parser_sqlview.add_option('-d', '--drop',action="store_true", dest="drop",
                          help = 'If true, will drop the view. (default false)')

def runner_sqlview(options, dbdescr, viewname):
    """
    Create/drop a view of the scheduled experiments.

    Usage: jobman sqlview <tablepath> <viewname>

    The jobs should be scheduled first with the sqlschedule command.
    Also, it is more interesting to execute it after some experiment have 
    finished.

    Assuming that a postgres database is running on port `port` of
    `host`, contains a database called `dbname` and that `user` has the
    permissions to create, read and modify tables on that database,
    tablepath should be of the following form:

        postgres://user:pass@host[:port]/dbname/tablename


    Example use:
        That was executed and at least one exeperiment was finished.
        jobman sqlschedule postgres://user:pass@host[:port]/dbname/tablename \\
            mymodule.my_experiment \\
            stopper::pylearn.stopper.nsteps \\ # use pylearn.stopper.nsteps
            stopper.n=10000 \\ # the argument "n" of nsteps is 10000
            lr=0.03
        Now this will create a view with a columns for each parameter and 
        key=value set in the state by the jobs.
        jobman sqlview postgres://user:pass@host[:port]/dbname/tablename viewname

        you can use the jobman.experiments.example1 as a working 
        mymodule.my_experiment
    """
    try:
        username, password, hostname, port, dbname, tablename \
            = sql.parse_dbstring(dbdescr)
    except Exception, e:
        raise UsageError('Wrong syntax for dbdescr',e)

    db = sql.postgres_serial(
        user = username,
        password = password,
        host = hostname,
        port = port,
        database = dbname,
        table_prefix = tablename)

    if options.drop:
        db.dropView(viewname)
    else:
        db.createView(viewname)

runner_registry['sqlview'] = (parser_sqlview, runner_sqlview)
