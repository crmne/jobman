
from __future__ import absolute_import

import os, sys, socket, datetime, copy, tempfile, shutil
from .dconfig import Config, load
from .api0 import sqlite_memory_db
from .api0 import postgres_serial


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

