from __future__ import absolute_import

import os, sys
from dconfig import Config, load

def run_job(fn=None, cwd=None,
        configpath = 'job_config.py',
        resultpath = 'job_result.py',
        stdoutpath = 'stdout',
        stderrpath = 'stderr',
        workdir = 'workdir'
        ):
    cwd = os.getcwd() if cwd is None else cwd

    #make absolute in case fn() chdirs
    resultpath = os.path.join(cwd, resultpath)

    stdout_orig = sys.stdout
    stderr_orig = sys.stderr
    try:
        sys.stdout = open(stdoutpath, 'w')
        sys.stderr = open(stderrpath, 'w')

        config = load(configpath)
        result = Config()
        wd = os.path.join(cwd, workdir)
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

        if fn is None:
            fn_module_name = config.job_module
            fn_symbol = config.job_symbol
            fn_module = __import__(fn_module_name)
            fn = getattr(fn_module, fn_symbol)

        fn(config=config, result=result)

        result.save(resultpath)

    finally:
        sys.stdout = stdout_orig
        sys.stderr = stderr_orig

