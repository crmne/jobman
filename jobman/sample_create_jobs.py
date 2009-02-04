import sys, subprocess

from .dconfig import Config
from .tools import Job # tools.py

###
### TODO: Fix this file, so it can work as a demo example
###

#
# Models
#

class OldSampleJob(Job):
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

        return True #restart this job



class SampleJobState():

    # default parameters for the job
    # config file parameters must be from this list.
    a = 0
    b = 0
    c = 'a'
    d = 0.0


sample_job_table = Table('my_table_for_testing', metadata,
        Column('a', Integer),
        Column('b', Float(53)))

metadata.create_all()

mapper(SampleJobState, sample_job_table)

s = SampleJobState()
s.a = 5
s.b = 8.2

if Session().query(SampleJobState).filter_by(a=5, b=8.2).any():
    break;
else:
    Session().save(s).commit()


class SampleJob(Job):
    """ Sample Job object.  Cut and paste this for your own experiments.  """

    def __init__(self, state):
        pass

    def start(self):
        pass

    def resume(self):
        pass

    def run(self, switch = lambda : 'continue'):
        # attributes that you set in 'results' will be saved to job_results.py, next to
        # job_config.py when this function returns.
        params.f = params.a * params.b

        
        # The current working directory (cwd) is writable and readable
        # it will also be left alone after the job terminates
        f = open('some_file', 'w')
        print >> f, "hello from the job?"
        while True:
            time.sleep(5)
            if switch() == 'stop':
                break

        return True #continue running if possible...

    def stop(self): 
        pass


    def run(self, state, dry_run=False):

def some_jobs(a = 0,
        b = 2):
    job_module = 'dbdict.sample_create_jobs'
    job_symbol = 'SampleJob'
    for c in ('a', 'b', 'c'):
        for d in [0.0, 9.9]:
            yield locals()

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

    j = SampleJob(SampleJobState())
    j.start()
    j.run()


