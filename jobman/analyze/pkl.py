"""analyze subcommands for manipulating jobs in a 'pkl' expdir
"""
import os, sys, cPickle
from ..expdir.pkl import (load_all, args_filename, results_filename,
        Multi, add_unique_grid,
        add_module_to_modulepack,
        todo_dir, done_dir, jobs_iter)
from ..analyze_runner import cmd

######################
# Commands for analyze
######################

@cmd
def list_jobs(exproot, **kwargs):
    """List jobs in the experiment"""
    for jobname, args, results in load_all(exproot):
        print jobname, args, results

@cmd
def list_dups(exproot, **kwargs):
    """List duplicate jobs in the experiment"""
    seen_args = []
    seen_names = []
    for jobname, args, results in load_all(exproot):
        if args in seen_args:
            print jobname, 'is dup of', seen_names[seen_args.index(args)]
        elif args != None:
            seen_args.append(args)
            seen_names.append(jobname)
@cmd
def del_dups(exproot, **kwargs):
    """Delete fresh duplicate jobs in the experiment"""
    seen_args = []
    seen_names = []
    for jobname, args, results in load_all(exproot):
        if args in seen_args:
            if os.listdir(os.path.join(exproot, jobname)) == [args_filename]:
                print jobname, 'is empty dup of', seen_names[seen_args.index(args)],
                print '...  deleting'
                os.remove(os.path.join(exproot, jobname, args_filename))
                os.rmdir(os.path.join(exproot, jobname))
            else:
                print jobname, 'is dup with files of', seen_names[seen_args.index(args)]
        elif args != None:
            seen_args.append(args)
            seen_names.append(jobname)



@cmd
def add_jobs(exproot, other_args, **kwargs):
    """Adds a grid of jobs to an experiment."""
    try:
        exproot, kwds = other_args[:2]
    except:
        print >> sys.stderr, ("Usage: jobman analyze insert_jobs <exproot>" 
        " '(key=val, key1=M(val0, val1, val2), ...)' "
        "[modpack0 [modpack1 [...]]]")
        return -1
    modpacks = other_args[2:]

    M = Multi
    grid_kwargs = eval('dict%s' % kwds)
    #print 'grid_kwargs', grid_kwargs
    #print 'modpacks', modpacks
    add_unique_grid(exproot, grid_kwargs, modpacks=modpacks)

@cmd
def add_modpack(exproot, other_args, **kwargs):
    """Add a modpack with snapshots of named modules"""
    packname = other_args[0]
    mods = other_args[1:]

    for modname in mods:
        print "Adding module", modname
        mod = __import__(modname)
        add_module_to_modulepack(mod, exproot, packname)

def _exists(path):
    return os.path.isfile(path) \
            or os.path.isdir(path) \
            or os.path.islink(path)

@cmd
def mark_all_todo(exproot, **kwargs):
    """Move all __done__ to __todo__, and create new symlinks for jobs not in __done__."""
    try:
        os.makedirs(os.path.join(exproot, todo_dir))
        os.makedirs(os.path.join(exproot, done_dir))
    except:
        pass

    for jobdir, fulljobdir in jobs_iter(exproot):
        if not _exists(os.path.join(exproot, todo_dir, jobdir)):
            if _exists(os.path.join(exproot, done_dir, jobdir)):
                # move from done -> todo
                os.rename(os.path.join(exproot, done_dir, jobdir),
                        os.path.join(exproot, todo_dir, jobdir))
            else:
                # create new link
                os.symlink(
                    os.path.join("..", jobdir), 
                    os.path.join(exproot, todo_dir, jobdir))

@cmd
def mark_all_done(exproot, **kwargs):
    """Move all __todo__ to __done__, and create new symlinks for jobs not in __todo__."""
    try:
        os.makedirs(os.path.join(exproot, todo_dir))
        os.makedirs(os.path.join(exproot, done_dir))
    except:
        pass

    for jobdir, fulljobdir in jobs_iter(exproot):
        if not _exists(os.path.join(exproot, done_dir, jobdir)):
            if _exists(os.path.join(exproot, todo_dir, jobdir)):
                # move from done -> todo
                os.rename(os.path.join(exproot, todo_dir, jobdir),
                        os.path.join(exproot, done_dir, jobdir))
            else:
                # create new link
                os.symlink(
                    os.path.join("..", jobdir), 
                    os.path.join(exproot, done_dir, jobdir))


@cmd
def cwd_args(**kwargs):
    """Print the args of a job in the cwd"""
    args = cPickle.load(open(args_filename))
    if isinstance(args, dict):
        keys = args.keys()
        keys.sort()
        for key in keys:
            print key, args[key]
    else:
        print args

@cmd
def cwd_results(**kwargs):
    """Print the results of a job in the cwd"""
    args = cPickle.load(open(results_filename))
    if isinstance(args, dict):
        keys = args.keys()
        keys.sort()
        for key in keys:
            print key, args[key]
    else:
        print args

