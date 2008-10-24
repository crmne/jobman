from __future__ import absolute_import

import os, sys, socket, datetime
from .dconfig import Config, load

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

    def print_config(self):
        for attr in self.options():
            print attr, '=', getattr(self, attr)

def run_job(fn=None, cwd=None,
        config_path = 'job_config.py',
        result_path = 'job_result.py',
        stdout_path = 'stdout',
        stderr_path = 'stderr',
        work_dir = 'workdir'
        ):
    cwd = os.getcwd() if cwd is None else cwd

class EXE(object):
    path_perf = 'job_perf.py'
    path_results = 'job_results.py'
    path_config = 'job_config.py'
    path_stdout = 'stdout'
    path_stderr = 'stderr'
    path_workdir = 'workdir'
    
    def __init__(self, exename):
        pass

    def _pop_cwd(self):
        try:
            cwd = sys.argv.pop(0)
            if not cwd.startswith('/'):
                cwd = os.path.join(os.getcwd(), cwd)
        except IndexError:
            cwd = os.getcwd()
        return cwd

    def _load_config(self, cwd = None):
        cwd = self._pop_cwd() if cwd is None else cwd
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

    def config(self):
        config = self._load_config()
        job_class = self._load_job_class(config)
        job = job_class(config)
        job.print_config()

    def run(self):
        cwd = self._pop_cwd()
        config = self._load_config(cwd)
        job_class = self._load_job_class(config)
        job = job_class(config)

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

            results = Config()
            job.run(results)
            results.save(os.path.join(cwd, self.path_results))

        finally:
            #put back stderr and stdout
            sys.stdout = stdout_orig
            sys.stderr = stderr_orig

    def defaults(self):
        job_class = self._load_job_class()
        job_class.print_defaults()

def standalone_run_job():
    exe = EXE(sys.argv.pop(0))
    try:
        cmd = sys.argv.pop(0)
        fn = getattr(exe, cmd)
    except IndexError:
        fn = getattr(exe, 'run')
    except AttributeError:
        print >> sys.stderr, "command not supported", cmd

    fn()




def standalone_query():
    pass
