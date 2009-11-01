"""utility functions for manipulating a 'pkl' experiment directory.
"""
import os, cPickle, shutil

args_filename = 'args.pkl'
results_filename = 'results.pkl'
new_jobname_format = 'job%06i'
modulepack_dir='__modulepack__'
todo_dir = '__todo__'
done_dir = '__done__'
no_sync_to_server = 'no_sync_to_server'

default_protocol=cPickle.HIGHEST_PROTOCOL


#########################
# Utilies inspecting jobs
#########################

def jobs_iter(exproot):
    """yields all pairs (jobdir, exproot/jobdir) """
    jobs = os.listdir(exproot)
    jobs.sort()
    for job in jobs:
        if job.startswith('__'):
            continue
        yield job, os.path.join(exproot, job)

def load_pkl(path):
    try:
        return cPickle.load(open(path))
    except (OSError, IOError), e:
        return None
def load_args(jobroot):
    return load_pkl(os.path.join(jobroot, args_filename))
def load_results(jobroot):
    return load_pkl(os.path.join(jobroot, results_filename))

def load_all(exproot):
    """Iterate over all (jobname, args, results) tuples
    """
    for jobname, jobpath in jobs_iter(exproot):
        yield jobname, load_args(jobpath), load_results(jobpath)


#############################
# Utilies for adding new jobs
#############################

def new_names(exproot, N, format=new_jobname_format):
    """
    """
    job_names = os.listdir(exproot)
    i = 0
    rvals = []
    while len(rvals) < N:
        name_i = format % i
        if name_i not in job_names:
            rvals.append(name_i)
        i += 1
    return rvals

def new_name(exproot, format=new_jobname_format):
    """
    """
    return new_names(exproot, 1, format=format)[0]

def add_named_jobs(exproot, name_list, args_list, 
        protocol=default_protocol,
        modpacks=[]):
    """
    """
    rval = []
    for name, args in zip(name_list, args_list):
        jobroot = os.path.join(exproot, name)
        os.mkdir(jobroot)
        for modpack in modpacks:
            os.symlink('../%s/%s'%(modulepack_dir,modpack), 
                    os.path.join(jobroot, 'PYTHONPATH.%s.%s'%(modpack, no_sync_to_server)))
        if args is not None:
            cPickle.dump(args, 
                    open(os.path.join(exproot, name, args_filename), 'w'),
                    protocol=protocol)
        rval.append((jobroot, args))
    return rval

def add_anon_jobs(exproot, args_list, protocol=default_protocol, modpacks=[]):
    """
    """
    return add_named_jobs(exproot, new_names(exproot, len(args_list)), args_list,
            protocol=protocol,
            modpacks=modpacks)

def add_unique_jobs(exproot, args_list, protocol=default_protocol, modpacks=[]):
    """Create jobdirs in exproot for each object in args_list

    :param exproot: path of the experiment root directory
    :param args_list: a list of objects to be picked as the args.pkl of each job.
    :param protocol: the cPickle protocol to be used for pickling
    :param modpacks: a list of names of module packages (see `add_module_to_modulepack`)

    """
    # load existing jobs from db
    existing_args = [args for args in [load_args(path) for (name,path) in jobs_iter(exproot)]
            if args is not None]
    try:
        # this will make the next step much faster if all the args are hashable
        set_args_list = set(args_list) # will raise if not all args hashable
        set_existing_args = set(existing_args)
        args_list = [a for a in args_list 
            if a not in set_existing_args # no dups with old args
            and a in set_args_list #no dups in new args
            ]
    except:
        args_list = [a for (i, a) in enumerate(args_list) 
            if a not in existing_args # no dups with old args
            and args_list.index(a) == i #no dups in new args
            ]
    return add_anon_jobs(exproot, args_list, protocol=protocol, modpacks=modpacks)

class Multi(list):
    """Object to represent a grid element that takes multiple values
    """
    def __init__(self, *args):
        list.__init__(self, args)

    def __str__(self):
        return "Multi{%s}" % list.__str__(self)

    def __repr__(self):
        return "Multi{%s}" % list.__repr__(self)

def add_unique_grid(exproot, grid_kwargs, protocol=default_protocol, modpacks=[]):
    """Add unique jobs filling out a grid.

    :param exproot:
    :param grid_kwargs: a dictionary of key-value pairs and key-Multi pairs.  The Multi values
    define the grid.
    :param protocol:
    :param modpacks:
    """
    jobs = [{}]
    for key, val in grid_kwargs.iteritems():
        if isinstance(val, Multi):
            jobs = sum(([dict(j, **{key:vv}) for j in jobs] for vv in val),
                    [])
        else:
            jobs = [dict(j, **{key:val}) for j in jobs]

    return add_unique_jobs(exproot, jobs, modpacks=modpacks)

######################################
# Utilities for adding module versions
######################################
def add_module_to_modulepack(module, exproot, name):
    """
    Copy `module` to <exproot>/<modulepack_dir>/<name>.

    A '__init__.py' file will be created in this folder if necessary.

    After this is done, a jobdir can demand specific versions of these modules by doing the
    following:

    1) job insertion script should add symlink to ../modulepack/name

    2) client code prepends that symlink to sys.path before importing module from the list.

    :param module: something that has been imported
    :param exproot: usual string
    :param name: the modulepack_dir subdir into which the copy will be placed.  This name is
    also what you should pass to the add_job commands so that they have correct symlinks.

    """

    modulepack = os.path.join(exproot, modulepack_dir, name)

    try:
        os.makedirs(modulepack)
    except OSError, e:
        if str(e).startswith('[Errno 17'):
            pass
        else:
            raise

    #touch the __init__ file
    open(os.path.join(modulepack, '__init__.py'),'a').close()

    # rsync from module.__path__ to modulepack
    if hasattr(module, '__path__'):
        if len(module.__path__) != 1:
            raise RuntimeError('Multiple paths in path... what does this mean?', module.__path__)
        curpath = module.__path__[0]
        cmd = 'rsync -ac --copy-unsafe-links %s/ %s/%s' % (curpath, modulepack, module.__name__)
    else:
        pyc = module.__file__
        cmd = 'rsync -ac --copy-unsafe-links %s/ %s/%s' % (pyc, modulepack, module.__name__)

    sts = os.system(cmd)
    if sts != 0:
        raise OSError('%s failed with status %s' %(cmd, sts))

def del_module_from_modulepack(module, exproot, name):
    """Erase a `module`'s copy from the modulepack `name` in `exproot`

    This undoes `add_module_to_modulepack`
    """
    if hasattr(module, '__path__'):
        if len(module.__path__) != 1:
            raise RuntimeError('Multiple paths in path... what does this mean?', module.__path__)
        lastpart = module.__path__[0]
        shutil.rmtree(os.path.join(exproot, modulepack_dir, name, lastpart))
    else:
        lastpart = module.__file__
        os.remove(os.path.join(exproot, modulepack_dir, name, lastpart))

