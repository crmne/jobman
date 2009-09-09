import sys, copy
from .tools import flatten
from .sql import db as sql_db
from .sql import parse_dbstring, EXPERIMENT, FUCKED_UP, insert_dict, hash_state

class Cmd(object):
    def __init__(self, f, short, long=""):
        self.f = f
        self.short = short
        self.long = long

    def __call__(self, *args, **kwargs):
        return self.f(*args, **kwargs)

    def desc_short(self):
        return self.short
    
    def desc_long(self):
        return self.long

cmd_dct = {}
def mydriver_cmd(f):
    cmd_dct[f.__name__] = Cmd(f, f.__doc__)
    return f

def mydriver_cmd_desc(short):
    def deco(f):
        cmd_dct[f.__name__] = Cmd(f, short)
        return f
    return deco


def help(db, **kwargs):
    """Print help for this program"""
    print "Usage: %s <cmd>" % sys.argv[0]
    #TODO
    print "Commands available:"
    for name, cmd in cmd_dct.iteritems():
        print "%20s - %s"%(name, cmd.desc_short())

@mydriver_cmd
def clear_db(db, **kwargs):
    """Delete all entries from the database """
    class y (object): pass
    really_clear_db = False
    n_records = len([i for i in db])
    try:
        if y is input('Are you sure you want to DELETE ALL %i records from %s? (N/y)' %
                (n_records, kwargs['dbstring'])):
            really_clear_db = True
    except:
        print 'not deleting anything'
    if really_clear_db:
        print 'deleting all...'
        for d in db:
            print d.id
            d.delete()

@mydriver_cmd_desc('Insert the job sequence into the database')
def insert(db, dbstring, argv, job_fn, job_dct_seq, exp_root, **kwargs):
    if ('-h' in argv or '' in argv):
        print """Ensure that all jobs in the job sequence have been inserted into the database.
    Optional arguments to cmd 'insert':
        --dry: don't actually commit any transaction with the database, just print how many duplicates there are.
        --dbi: print to stdout the command necessary to launch all new jobs using dbidispatch.
        """
        return
    dryrun = ('--dry' in argv)
    didsomething = True
    pos = 0
    S = db.session()
    for i, experiment in enumerate(job_dct_seq):
        #TODO: use hashlib, not the builtin hash function.  Requires changing in .sql as well, maybe more places?  
        # Also, will break old experiment code because inserts will all work even though jobs have already run.
        state = dict(flatten(experiment))
        experiment_name = job_fn.__module__ + '.' + job_fn.__name__
        if EXPERIMENT in state:
            if state[EXPERIMENT] != experiment_name:
                raise Exception('Inconsistency: state element %s does not match experiment %s' %(EXPERIMENT, experiment_name))
        else:
            state[EXPERIMENT] = experiment_name

        jobhash = hash_state(state)

        if dryrun:
            # TODO: detect if experiment is a duplicate or not
            if (None is S.query(db._Dict).filter(db._Dict.hash==jobhash).filter(db._Dict.status!=FUCKED_UP).first()):
                is_dup = False

            else:
                is_dup = True
            #print 'DEBUG', inserted, jobhash
        else:
            if None is insert_dict(state, db, force_dup=False, priority=1, session=S):
                is_dup = True
            else:
                is_dup = False

        if is_dup:
            sys.stdout.write('-')
        else:
            pos += 1
            sys.stdout.write('.')

        #print ' #', jobhash,':', experiment
        #print '\n'

    sys.stdout.write('\n')
    S.close()
    print '***************************************'
    if dryrun:
        print '*              Summary [DRY RUN]      *'
    else:
        print '*              Summary                *'
    print '***************************************'
    print '* Inserted %i/%i jobs in database' % (pos,i+1)
    print '***************************************'

    if '--dbi' in sys.argv:
        dbi_index = argv.index('--dbi')
        cmd = 'dbidispatch --repeat_jobs=%i %s' %(pos, argv[dbi_index+1])
        print 'TODO: run ', cmd, 'jobman sql', dbstring, exp_root

def create_view(db, **kwargs):
    """Create a view (WRITEME)"""
    db.createView(kwargs['tablename'] + 'view')

@mydriver_cmd
def status(db, **kwargs):
    sts = {0:0, 1:0, 2:0, 666:0}
    for d in db:
        cnt = sts.get(d['jobman.status'], 0)
        sts[d['jobman.status']] = cnt + 1

    print 'QUEUED  :', sts[0]; del sts[0]
    print 'RUNNING :', sts[1]; del sts[1]
    print 'DONE    :', sts[2]; del sts[2]
    print 'MESSED  :', sts[666]; del sts[666]

    if sts:
        print 'WARNING: other status counts:', sts

def main(argv, dbstring, exp_root, job_fn, job_dct_seq):
    db = sql_db(dbstring)
    username, password, hostname, dbname, tablename = parse_dbstring(dbstring)

    job_dct_seq = tuple(job_dct_seq)

    try:
        cmd = cmd_dct[argv[1]]
    except:
        cmd = help
    cmd(**locals())

