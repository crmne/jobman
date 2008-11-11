from __future__ import absolute_import

import os, sys, socket, datetime, copy, tempfile, shutil
from .dconfig import Config, load
from .api0 import sqlite_memory_db
from .api0 import postgres_serial


def add_experiments_to_db(exp_cls, jobs, db, verbose=0, add_dups=False, type_check=None):
    """Add experiments paramatrized by exp_cls and jobs[i] to database db.

    Default behaviour is to ignore jobs which are already in the database.

    If type_check is a class (instead of None) then it will be used as a type declaration for
    all the elements in each job dictionary.  For each key,value pair in the dictionary, there
    must exist an attribute,value pair in the class meeting the following criteria:
    the attribute and the key are equal, and the types of the values are equal.

    :param exp_cls: The Experiment class to run these experiments.
    :param jobs: The parameters of experiments to run.
    :type jobs: an iterable object over dictionaries
    :param verbose: print which jobs are added and which are skipped
    :param add_dups: False will ignore a job if it matches (on all items()) with a db entry.
    :type add_dups: Bool

    :returns: list of (Bool,job[i]) in which the flags mean the corresponding job actually was
    inserted.

    """
    rval = []
    for job in jobs:
        job = copy.copy(job)
        do_insert = add_dups or (None is db.query(**job).first())

        if do_insert:
            if type_check:
                for k,v in job.items():
                    if type(v) != type(getattr(type_check, k)):
                        raise TypeError('Experiment contains value with wrong type',((k,v),
                            type(getattr(type_check, k))))

            job['dbdict_status'] = RunExperimentSql.NEW
            job['dbdict_symbol'] = exp_cls.__name__
            job['dbdict_module'] = exp_cls.__module__
            job['dbdict_priority'] = 1.0
            if verbose:
                print 'ADDING  ', job
            db.insert(job)
            rval.append((True, job))
        else:
            if verbose:
                print 'SKIPPING', job
            rval.append((False, job))


class Job(object):
    @classmethod
    def is_a_default_option(cls, key, val):
        if callable(val):
            return False
        if key.startswith('_'):
            return False
        if key in ('job_module', 'job_symbol'):
            return False
        return True

    @classmethod
    def options(cls):
        opts = [attr for attr in dir(cls) 
            if cls.is_a_default_option(attr, getattr(cls, attr))]
        opts.sort()
        return opts


    def __init__(self, config):
        for key, val in config.items():
            if self.is_a_default_option(key, val):
                if hasattr(self, key):
                    setattr(self, key, val)
                else:
                    raise AttributeError("Job has no such option:", key)

    @classmethod
    def print_defaults(cls):
        for attr in cls.options():
            print attr, '=', getattr(cls, attr)

    def print_config(self, outfile=sys.stdout):
        for attr in self.options():
            print >> outfile, attr, '=', getattr(self, attr)

    def save_config(self, outfile=sys.stdout):
        """Like print_config, but with parsable format rather than human-readable format"""
        kwargs = dict([(attr,getattr(self, attr)) for attr  in self.options()])
        Config(**kwargs).save_file(outfile)

def run_job(fn=None, cwd=None,

        config_path = 'job_config.py',
        result_path = 'job_result.py',
        stdout_path = 'stdout',
        stderr_path = 'stderr',
        work_dir = 'workdir'
        ):
    cwd = os.getcwd() if cwd is None else cwd

def _pop_cwd():
    try:
        cwd = sys.argv.pop(0)
        if not cwd.startswith('/'):
            cwd = os.path.join(os.getcwd(), cwd)
    except IndexError:
        cwd = os.getcwd()
    return cwd


class RunJob(object):
    """
    """

    path_perf = 'job_perf.py'
    path_results = 'job_results.py'
    path_config = 'job_config.py'
    path_fullconfig = 'job_fullconfig.py'
    path_stdout = 'stdout'
    path_stderr = 'stderr'
    path_workdir = 'workdir'
    
    def __init__(self, exename):
        pass

    def _load_config(self, cwd = None):
        cwd = _pop_cwd() if cwd is None else cwd
        config = load(os.path.join(cwd, self.path_config))
        return config
    
    def _load_job_class(self, config=None):
        config = self._load_config() if config is None else config
        job_module_name = config.job_module
        job_symbol = config.job_symbol
        #level=0 -> absolute imports
        #fromlist=[None] -> evaluate the rightmost element of the module name, when it has
        #                   dots (e.g., "A.Z" will return module Z)
        job_module = __import__(job_module_name, fromlist=[None], level=0)
        try:
            job_class = getattr(job_module, job_symbol)
        except:
            print >> sys.stderr, "failed to load job class:", job_module_name, job_symbol
            raise
        return job_class

    def print_config(self):
        config = self._load_config()
        job_class = self._load_job_class(config)
        job = job_class(config)
        job.print_config()

    def run(self):
        cwd = _pop_cwd()
        config = self._load_config(cwd)
        job_class = self._load_job_class(config)
        job = job_class(config)

        job.save_config(open(os.path.join(cwd, self.path_fullconfig),'w'))

        perf = Config()
        perf.host_name = socket.gethostname()
        perf.start_time = str(datetime.datetime.now())
        perf.save(os.path.join(cwd, self.path_perf))

        stdout_orig = sys.stdout
        stderr_orig = sys.stderr

        try:
            sys.stdout = open(os.path.join(cwd, self.path_stdout), 'w')
            sys.stderr = open(os.path.join(cwd, self.path_stderr), 'w')

            #
            # mess around with the working directory
            #

            wd = os.path.join(cwd, self.path_workdir)
            try:
                os.mkdir(wd)
            except OSError, e:
                print >> sys.stderr, "trouble making wordking directory:"
                print >> sys.stderr, e
                print >> sys.stderr, "ignoring error and proceeding anyway"
            try:
                os.chdir(wd)
            except:
                pass

            print >> sys.stderr, "cwd:", os.getcwd()

            #
            # run the job...
            #

            #TODO load the results file before running the job, to resume job
            results = Config()

            job_rval = job.run(results)

            #TODO: use the return value to decide whether the job should be resumed or not,
            #      False  means run is done
            #      True   means the job has yielded, but should be continued
            results.save(os.path.join(cwd, self.path_results))

        finally:
            #put back stderr and stdout
            sys.stdout = stdout_orig
            sys.stderr = stderr_orig
            perf.end_time = str(datetime.datetime.now())
            perf.save(os.path.join(cwd, self.path_perf))

    def defaults(self):
        job_class = self._load_job_class()
        job_class.print_defaults()

def standalone_run_job():
    exe = RunJob(sys.argv.pop(0))
    try:
        cmd = sys.argv.pop(0)
        fn = getattr(exe, cmd)
    except IndexError:
        fn = getattr(exe, 'run')
    except AttributeError:
        print >> sys.stderr, "command not supported", cmd

    fn()

def build_db(cwd, db=None):
    """WRITEME"""
    db = sqlite_memory_db() if db is None else db
    for e in os.listdir(cwd):
        e = os.path.join(cwd, e)
        try:
            e_config = open(os.path.join(e, 'job_config.py'))
        except:
            e_config = None

        try: 
            e_sentinel = open(os.path.join(e, '__jobdir__'))
        except:
            e_sentinel = None

        if not (e_config or e_sentinel):
            continue #this is not a job dir

        if e_config:
            e_config.close()
            config = load(os.path.join(e, 'job_config.py'))
            kwargs = copy.copy(config.__dict__)
            try:
                results = load(os.path.join(e, 'job_results.py'))
                kwargs.update(results.__dict__)
            except:
                pass
            try:
                perf = load(os.path.join(e, 'job_perf.py'))
                kwargs.update(perf.__dict__)
            except:
                pass

            #TODO: this is a backward-compatibility hack for AISTATS*09
            if 'perf_jobdir' not in kwargs:
                kwargs['perf_jobdir'] = e
            if 'perf_workdir' not in kwargs:
                kwargs['perf_workdir'] = os.path.join(e, 'workdir')

            entry = db.insert(kwargs)

        if e_sentinel:
            print >> sys.stderr, "NOT-IMPLEMENTED: RECURSION INTO SUBDIRECTORY", e
    return db


class RunQuery(object):

    def __init__(self, exename):
        pass

    def run(self):
        cwd = _pop_cwd()
        db = build_db(cwd)
        for entry in db.query().all():
            print entry.items()


def standalone_query():
    exe = RunQuery(sys.argv.pop(0))
    try:
        cmd = sys.argv.pop(0)
        fn = getattr(exe, cmd)
    except IndexError:
        fn = getattr(exe, 'run')
    except AttributeError:
        print >> sys.stderr, "command not supported", cmd

    fn()

_sql_usage = """Usage:
    dbdict-experiment sql <dbstring> <tablename>
"""

class RunExperimentSql(object):
    module = 'dbdict_module'
    symbol = 'dbdict_symbol'
    status = 'dbdict_status'
    priority = 'dbdict_priority'
    host = 'dbdict_hostname'
    host_workdir = 'dbdict_host_workdir'
    push_error = 'dbdict_push_error'

    NEW = 0
    """dbdict_status == NEW means a experiment has never run"""

    RUNNING = 1
    """dbdict_status == RUNNING means a experiment is running on dbdict_hostname"""

    DONE = 2
    """dbdict_status == DONE means a experiment has completed (not necessarily successfully)"""

    STOPPED = 3
    """dbdict_status == STOPPED means a experiment has run, but not completed and is not
    currently running"""

    STOPPED_PRIORITY = 2.0
    """Stopped experiments are marked with this priority"""

    def __init__(self):
        try:
            self.hostname = sys.argv.pop(0)
            assert self.hostname != 'help'
            self.username = sys.argv.pop(0)
            self.dbname = sys.argv.pop(0)
            self.password = open(os.getenv('HOME')+'/.dbdict_%s'%self.dbname).readline()[:-1]
            self.tablename = sys.argv.pop(0)
            self.experiment_root = sys.argv.pop(0)
        except Exception, e:
            print >> sys.stderr, e
            self.help()
            return

        #TODO: THIS IS A GOOD IDEA RIGHT?
        #   It makes module-lookup work based on cwd-relative paths
        #   But possibly has really annoying side effects?  Is there a cleaner
        #   way to change the import path just for loading the experiment class?
        sys.path.insert(0, os.getcwd())

        while True:
            dct = self.get_a_dbdict_to_run()
            if dct is None: break

            try:
                #
                # chdir to a temp folder
                # 
                workdir = tempfile.mkdtemp()
                print >> sys.stderr, "INFO WORKDIR: ", workdir
                os.chdir(workdir)
        
                # not sure where else to put this...
                dct[self.host] = socket.gethostname()
                dct[self.host_workdir] = workdir


                run_rval = self.run_dct(dct)

                #
                # Cleanup the tempdir
                # TODO: put this in a 'finally' block?
                #
                shutil.rmtree(workdir, ignore_errors=True)
            except:
                dct[self.status] = self.DONE
                dct[self.priority] = None
                raise

            if run_rval:
                #mark the job as being done
                dct[self.status] = self.STOPPED
                dct[self.priority] = self.STOPPED_PRIORITY
            else:
                #mark the job as being done
                dct[self.status] = self.DONE
                dct[self.priority] = None

    def get_a_dbdict_to_run(self):
        #get a dictionary
        print >> sys.stderr, """#TODO: use the priority field, not the status."""
        print >> sys.stderr, """#TODO: ignore entries with key self.push_error."""

        db = postgres_serial(user=self.username, 
                password=self.password, 
                database=self.dbname,
                host=self.hostname,
                table_prefix=self.tablename)

        top_new = db.query(dbdict_status=self.NEW).first()
        top_stopped = db.query(dbdict_status=self.STOPPED).first()

        return top_stopped or top_new

    def experiment_location(self, dct):
        """The network location (directory) at which experiment-related files are stored.

        :returns: "<host>:<path>", of the sort used by ssh and rsync.
        """
        return os.path.join(self.experiment_root, self.dbname, self.tablename, str(dct.id))

    def pull_cwd(self, dct):
        """pull from the experiment root tree to cwd"""
        rsync_rval = os.system('rsync -r "%s/*" .' % self.experiment_location(dct))
        if rsync_rval != 0:
            raise Exception('rsync failure', rsync_rval)

    def push_cwd(self, dct):
        """push from cwd to the experiment root tree"""
        loc = self.experiment_location(dct)
        marker = loc.index(':')
        host = loc[:marker]
        ssh_cmd = 'ssh ' + host + ' "mkdir -p \'%s\'"'%loc[marker+1:]
        ssh_rval = os.system(ssh_cmd)
        if 0 != ssh_rval:
            raise Exception('ssh failure', (ssh_rval, ssh_cmd))

        rsync_cmd = 'rsync -r . "%s"' % self.experiment_location(dct)
        rsync_rval = os.system(rsync_cmd)
        if rsync_rval != 0:
            raise Exception('rsync failure', (rsync_rval, rsync_cmd))

    def run_dct(self, dct):

        #
        # load the experiment class 
        #
        dbdict_module_name = dct[self.module]
        dbdict_symbol = dct[self.symbol]

        dbdict_module = __import__(dbdict_module_name, fromlist=[None], level=0)
        try:
            dbdict_class = getattr(dbdict_module, dbdict_symbol)
        except:
            print >> sys.stderr, "FAILED to load job class:", dbdict_module_name, dbdict_symbol
            raise
        print >> sys.stderr, 'INFO RUNNING', dbdict_class
        print >> sys.stderr, 'INFO CONFIG', dct.items()

        #
        #this proxy object lets experiments use the dct like a state object
        #
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

        #
        # instantiate and run the experiment
        #

        if self.STOPPED == dct[self.status]:
            self.pull_cwd(dct)
            experiment = dbdict_class(Proxy())
            experiment.resume()
        elif self.NEW == dct[self.status]:
            experiment = dbdict_class(Proxy())
            experiment.start()
        else:
            raise Exception('unexpected dct[%s]' % self.status, dct.items())

        print >> sys.stderr, "#TODO: upgrade channel to check for TERM signals, job timeout"

        def channel():
            #return 'stop' to tell run() to return ASAP
            return None

        run_rval = experiment.run(channel)

        #
        # push the results back to the experiment_root
        #
        try:
            self.push_cwd(dct)
        except Exception, e:
            dct[self.push_error] = str(e)
            raise

        return run_rval

    def help(self):
        print _sql_usage # string not inlined so as not to fool VIM's folding mechanism

class RunFile(object):
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

def dispatch_cmd(self, stack=sys.argv):
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


_experiment_usage = """Usage:
    dbdict-experiment <cmd>

Commands:

    help    Print help. (You're looking at it.)

    dbdict  Obtain experiment configuration by loading a dbdict file
    file    Obtain experiment configuration by evaluating the commandline
    sql     Obtain experiment configuration by querying an sql database

Help on individual commands might be available by typing 'dbdict-experiment <cmd> help'

"""

class RunExperiment(object):
    """This class handles the behaviour of the dbdict-run script."""
    def __init__(self):
        exe = sys.argv.pop(0)
        dispatch_cmd(self)

    sql = RunExperimentSql

    file = RunFile

    def help(self):
        print _experiment_usage # string not inlined so as not to fool VIM's folding mechanism
