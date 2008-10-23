
import os
import basicdict

def run_job(fn, cwd=None,
        configpath = 'job_config',
        resultpath = 'job_result',
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

        config = basicdict.load(configpath)
        result = basicdict.BasicDict()
        wd = os.path.join(cwd, workdir)
        try:
            os.mkdir(wd)
        except OSError, e:
            print >> sys.stderr, "trouble making wordking directory:"
            print >> sys.stderr, e
            print >> sys.stderr, "ignoring error and proceeding anyway"
        fn(config=config, result=result, workdir=wd)

        result.save(resultpath)

    finally:
        sys.stdout = stdout_orig
        sys.stderr = stderr_orig

