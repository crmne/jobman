import sys, subprocess

from dbdict.dconfig import Config
#
#
# Models
#
#
def some_jobs(a = 0,
        b = 2):
    for c in ('a', 'b', 'c'):
        for d in [0.0, 9.9]:
            yield Config(locals())

def create():
    """Create a set of job directories"""
    generator = some_jobs #eval(sys.argv[1])
    jobs = []
    dispatch = open('jobs','w')

    for config in generator:
        #print '   config', config
        configdir = 'job_%016x'% abs(hash(config))
        print >> dispatch, "dbdict-run-job run", configdir
        jobs.append(configdir)
        create_dirs = True
        dry_run = 0
        if not dry_run:
            if create_dirs and subprocess.call(('mkdir', configdir)):
                print 'Error creating directory: ', configdir
            else:
                #no problem creating the directory
                config.save(configdir + '/job_config.py')
        else:
            #print configdir
            pass
    dispatch.close()

if __name__ == '__main__':
    create()


