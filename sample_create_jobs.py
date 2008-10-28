import sys, subprocess

from .dconfig import Config
from .tools import Job # tools.py

#
# Models
#

class SampleJob(Job):
    """ Sample Job object.  Cut and paste this for your own experiments.  """

    # default parameters for the job
    # config file parameters must be from this list.
    a = 0
    b = 0
    c = 'a'
    d = 0.0

    def run(self, results, dry_run=False):
        # attributes that you set in 'results' will be saved to job_results.py, next to
        # job_config.py when this function returns.
        results.f = self.a * self.b

        # The current working directory (cwd) is writable and readable
        # it will also be left alone after the job terminates
        f = open('some_file', 'w')
        print >> f, "hello from the job?"

def some_jobs(a = 0,
        b = 2):
    job_module = 'dbdict.sample_create_jobs'
    job_symbol = 'SampleJob'
    for c in ('a', 'b', 'c'):
        for d in [0.0, 9.9]:
            yield Config(locals())

def create(generator):
    """Create a set of job directories"""
    jobs = []
    dispatch = sys.stdout #open('jobs','w')

    for config in generator:
        #print '   config', config
        configdir = 'job_%016x'% abs(hash(config))
        jobs.append(configdir)
        create_dirs = True
        dry_run = 0
        if not dry_run:
            if create_dirs and subprocess.call(('mkdir', configdir)):
                print >> sys.stderr, 'Error creating directory: ', configdir
            else:
                #no problem creating the directory
                config.save(configdir + '/job_config.py')
                print >> dispatch, "dbdict-run-job run", configdir
        else:
            #print configdir
            pass
    dispatch.close()

if __name__ == '__main__':
    create(some_jobs())


