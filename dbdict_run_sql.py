"""Code related to using the api0 dictionary table to run experiments.

This module has been tested with postgres.

"""

import sys, os, socket, tempfile, shutil, copy, time
import numpy # for the random wait time in book_unstarted_trial

import sqlalchemy
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import eagerload
import psycopg2, psycopg2.extensions 

from .api0 import db_from_engine, postgres_db
from .tools import run_state, DictProxyState, COMPLETE, INCOMPLETE, SYMBOL, MODULE
from .dconfig import save_items

# _TEST CONCURRENCY
# To ensure that concurrency is handled properly by the consumer (book_dct)
# set this flag to True and (manually) run the following test.
#
# Launch two processes side by side, starting one of them a few seconds after the other.
# There is an extra sleep(10) that will delay each process's job dequeue.
#
# You should see that the process that started first gets the job,
# and the process that started second tries to get the same job, 
# fails, and then gets another one instead.
_TEST_CONCURRENCY = False

_help = """Usage:
dbdict-run sql postgres://<user>:<pass>@<host>/<db>/<api0-table> <experiment-root>

    user        - postgres username
    pass        - password
    hostname    - the network address of the host on which a postgres server is 
                  running (on port ??)
    database    - a database served by the postgres server on <hostname>
    api0-table  - the name (actually, table_prefix) associated with tables, 
                  created by dbdict.api0.

    experiment-root - a local or network path.  Network paths begin with ssh://
                  E.g. /tmp/blah
                       ssh://mammouth:blah
                       ssh://foo@linux.org:/tmp/blah

Experiment-root is used to store the file results of experiments.  If a job with a given <id>
creates file 'a.txt', and directory 'b' with file 'foo.py', then these will be rsync'ed to the
experiment-root when job <id> has finished running.  They will be found here:

    <experiment-root>/<db>/<api0-table>/<id>/workdir/a.txt
    <experiment-root>/<db>/<api0-table>/<id>/workdir/b/foo.py

Files 'stdout', 'stderr', and 'state.py' will be created.

    <experiment-root>/<db>/<api0-table>/<id>/stdout   - opened for append
    <experiment-root>/<db>/<api0-table>/<id>/stderr   - opened for append
    <experiment-root>/<db>/<api0-table>/<id>/state.py - overwritten with database version

If a job is restarted or resumed, then those files are rsync'ed back to the current working
directory, and stdout and stderr are re-opened for appending.  When a resumed job stops for
the second (or more) time, the cwd is rsync'ed back to the experiment-root.  In this way, the
experiment-root accumulates the results of experiments that run.

"""

STATUS = 'dbdict_sql_status'
PRIORITY = 'dbdict_sql_priority'
HOST = 'dbdict_sql_hostname'
HOST_WORKDIR = 'dbdict_sql_host_workdir'
PUSH_ERROR = 'dbdict_sql_push_error'

START = 0
"""dbdict_status == START means a experiment is ready to run"""

RUNNING = 1
"""dbdict_status == RUNNING means a experiment is running on dbdict_hostname"""

DONE = 2
"""dbdict_status == DONE means a experiment has completed (not necessarily successfully)"""

RESTART_PRIORITY = 2.0
"""Stopped experiments are marked with this priority"""


def postgres_serial(user, password, host, database, **kwargs):
    """Return a DbHandle instance that communicates with a postgres database at transaction
    isolation_level 'SERIALIZABLE'.

    :param user: a username in the database
    :param password: the password for the username
    :param host: the network address of the host on which the postgres server is running
    :param database: a database served by the postgres server
    
    """
    this = postgres_serial

    if not hasattr(this,'engine'):
        def connect():
            c = psycopg2.connect(user=user, password=password, database=database, host=host)
            c.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
            return c
        pool_size = 0
        this.engine = create_engine('postgres://'
                ,creator=connect
                ,pool_size=0 # should force the app release connections
                )

    db = db_from_engine(this.engine, **kwargs)
    db._is_serialized_session_db = True
    return db


def add_experiments_to_db(exp_cls, jobs, db, verbose=0, add_dups=False, type_check=None):
    """Add experiments paramatrized by exp_cls and jobs[i] to database db.

    Default behaviour is to ignore jobs which are already in the database.

    If type_check is a class (instead of None) then it will be used as a type declaration for
    all the elements in each job dictionary.  For each key,value pair in the dictionary, there
    must exist an attribute,value pair in the class meeting the following criteria:
    the attribute and the key are equal, and the types of the values are equal.

    :param exp_cls: The Experiment class to run these experiments.
    :param jobs: The parameters of experiments to run.
    :type jobs: an iterable object over dictionaries
    :param verbose: print which jobs are added and which are skipped
    :param add_dups: False will ignore a job if it matches (on all items()) with a db entry.
    :type add_dups: Bool

    :returns: list of (Bool,job[i]) in which the flags mean the corresponding job actually was
    inserted.

    """
    rval = []
    for job in jobs:
        job = copy.copy(job)
        do_insert = add_dups or (None is db.query(**job).first())

        if do_insert:
            if type_check:
                for k,v in job.items():
                    if type(v) != getattr(type_check, k):
                        raise TypeError('Experiment contains value with wrong type',
                                ((k,v), getattr(type_check, k)))

            job[STATUS] = START
            job[SYMBOL] = exp_cls.__name__
            job[MODULE] = exp_cls.__module__
            job[PRIORITY] = 1.0
            if verbose:
                print 'ADDING  ', job
            db.insert(job)
            rval.append((True, job))
        else:
            if verbose:
                print 'SKIPPING', job
            rval.append((False, job))


def book_dct_postgres_serial(db, retry_max_sleep=10.0, verbose=0):
    """Find a trial in the lisa_db with status START.

    A trial will be returned with dbdict_status=RUNNING.

    Returns None if no such trial exists in DB.

    This function uses a serial access to the lisadb to guarantee that no other
    process will retrieve the same dct.  It is designed to facilitate writing
    a "consumer" for a Producer-Consumer pattern based on the database.

    """
    print >> sys.stderr, """#TODO: use the priority field, not the status."""
    print >> sys.stderr, """#TODO: ignore entries with key PUSH_ERROR."""

    s = db._session

    # NB. we need the query and attribute update to be in the same transaction
    assert s.autocommit == False 

    dcts_seen = set([])
    keep_trying = True

    dct = None
    while (dct is None) and keep_trying:
        #build a query
        q = s.query(db._Dict)
        q = q.options(eagerload('_attrs')) #hard-coded in api0
        q = q.filter(db._Dict._attrs.any(name=STATUS, fval=START))

        #try to reserve a dct
        try:
            #first() may raise psycopg2.ProgrammingError
            dct = q.first()

            if dct is not None:
                assert (dct not in dcts_seen)
                if verbose: print 'book_unstarted_dct retrieved, ', dct
                dct._set_in_session(STATUS, RUNNING, s)
                if 1:
                    if _TEST_CONCURRENCY:
                        print >> sys.stderr, 'SLEEPING BEFORE BOOKING'
                        time.sleep(10)

                    #commit() may raise psycopg2.ProgrammingError
                    s.commit()
                else:
                    print >> sys.stderr, 'DEBUG MODE: NOT RESERVING JOB!', dct
                #if we get this far, the job is ours!
            else:
                # no jobs are left
                keep_trying = False
        except (psycopg2.OperationalError,
                sqlalchemy.exceptions.ProgrammingError), e:
            #either the first() or the commit() raised
            s.rollback() # docs say to do this (or close) after commit raises exception
            if verbose: print 'caught exception', e
            if dct:
                # first() succeeded, commit() failed
                dcts_seen.add(dct)
                dct = None
            wait = numpy.random.rand(1)*retry_max_sleep
            if verbose: print 'another process stole our dct. Waiting %f secs' % wait
            time.sleep(wait)
    return dct

def book_dct(db):
    print >> sys.stderr, """#TODO: use the priority field, not the status."""
    print >> sys.stderr, """#TODO: ignore entries with key self.push_error."""

    return db.query(dbdict_status=START).first()

def parse_dbstring(dbstring):
    postgres = 'postgres://'
    assert dbstring.startswith(postgres)
    dbstring = dbstring[len(postgres):]

    #username_and_password
    colon_pos = dbstring.find('@')
    username_and_password = dbstring[:colon_pos]
    dbstring = dbstring[colon_pos+1:]

    colon_pos = username_and_password.find(':')
    if -1 == colon_pos:
        username = username_and_password
        password = None
    else:
        username = username_and_password[:colon_pos]
        password = username_and_password[colon_pos+1:]
    
    #hostname
    colon_pos = dbstring.find('/')
    hostname = dbstring[:colon_pos]
    dbstring = dbstring[colon_pos+1:]

    #dbname
    colon_pos = dbstring.find('/')
    dbname = dbstring[:colon_pos]
    dbstring = dbstring[colon_pos+1:]

    #tablename
    tablename = dbstring

    if password is None:
        password = open(os.getenv('HOME')+'/.dbdict_%s'%dbname).readline()[:-1]
    if False:
        print 'USERNAME', username
        print 'PASS', password
        print 'HOST', hostname
        print 'DB', dbname
        print 'TABLE', tablename

    return username, password, hostname, dbname, tablename

class ExperimentLocation(object):
    def __init__(self, root, dbname, tablename, id):
        ssh_prefix='ssh://'
        if root.startswith(ssh_prefix):
            root = root[len(ssh_prefix):]
            #at_pos = root.find('@')
            colon_pos = root.find(':')
            self.host, self.path = root[:colon_pos], root[colon_pos+1:]
        else:
            self.host, self.path = '', root
        self.dbname = dbname
        self.tablename = tablename
        self.id = id

    def rsync(self, direction):
        """The directory at which experiment-related files are stored.

        :returns: "<host>:<path>", of the sort used by ssh and rsync.
        """
        path = os.path.join(
                ':'.join([self.host, self.path]), 
                self.dbname, 
                self.tablename, 
                self.id)

        if direction == 'push':
            rsync_cmd = 'rsync -r * "%s"' % path
        elif direction == 'pull':
            rsync_cmd = 'rsync -r "%s/*" .' % path
        else:
            raise Exception('invalid direction', direction)

        rsync_rval = os.system(rsync_cmd)
        if rsync_rval != 0:
            raise Exception('rsync failure', (rsync_rval, rsync_cmd))

    def pull(self):
        return self.rsync('pull')

    def push(self):
        return self.rsync('push')

    def touch(self):
        host = self.host
        path = os.path.join(self.path, self.dbname, self.tablename, self.id)
        ssh_cmd = ('ssh %(host)s  "mkdir -p \'%(path)s\' && cd \'%(path)s\' '
        '&& touch stdout stderr && mkdir -p workdir"' % locals())
        ssh_rval = os.system(ssh_cmd)
        if 0 != ssh_rval:
            raise Exception('ssh failure', (ssh_rval, ssh_cmd))

    def delete(self):
        #something like ssh %s 'rm -Rf %s' should work, but it's pretty scary...
        raise NotImplementedError()

def run_sql():
    try:
        username, password, hostname, dbname, tablename = parse_dbstring(sys.argv.pop(0))
    except Exception, e:
        print >> sys.stderr, e
        print >> sys.stderr, _help
        raise

    #set experiment_root
    try:
        exproot = sys.argv.pop(0)
    except:
        exproot = os.getcwd()

    #TODO: THIS IS A GOOD IDEA RIGHT?
    #   It makes module-lookup work based on cwd-relative paths
    #   But possibly has really annoying side effects?  Is there a cleaner
    #   way to change the import path just for loading the experiment class?
    sys.path.insert(0, os.getcwd())

    #TODO: refactor this so that we can use any kind of database (not just postgres)

    #a serialized session is necessary for jobs not be double-booked by listeners running
    #in parallel on the cluster.
    db = postgres_serial(user=username, 
            password=password, 
            host=hostname,
            database=dbname,
            table_prefix=tablename)

    while True:
        dct = book_dct_postgres_serial(db, verbose=1)
        if dct is None:
            break

        try:
            #
            # chdir to a temp folder
            # 
            workdir = tempfile.mkdtemp()
            print >> sys.stderr, "INFO RUNNING ID %i IN %s" % (dct.id, workdir)
            os.chdir(workdir)
    
            # not sure where else to put this...
            dct[HOST] = socket.gethostname()
            dct[HOST_WORKDIR] = workdir

            exploc = ExperimentLocation(exproot, dbname, tablename, str(dct.id))

            #
            # pull cwd contents
            #
            exploc.touch()
            exploc.pull()

            #
            # run the experiment
            # 
            old_stdout, old_stderr = sys.stdout, sys.stderr
            sys.stdout, sys.stderr = open('stdout', 'a+'), open('stderr', 'a+')
            assert RUNNING == dct[STATUS] #set by get_dct
            os.chdir(os.path.join(workdir, 'workdir'))
            try:
                run_rval = run_state(DictProxyState(dct))
            except Exception, e:
                run_rval = COMPLETE
                print >> sys.stderr, 'Exception:', e
                print >> old_stderr, '#TODO: print a bigger traceback to stderr'
            sys.stdout, sys.stderr = old_stdout, old_stderr

            #
            # push the results back to the experiment_root
            #
            try:
                os.chdir(workdir)
                #pickle the state #TODO: write it human-readable
                #cPickle.dump(dict((k,v) for k,v in dct.items()), open('state.pickle','w'))
                save_items(dct.items(), open('state.py', 'w'))
                exploc.push()
            except Exception, e:
                dct[PUSH_ERROR] = str(e)
                raise

            # Cleanup the tempdir
            # TODO: put this in a 'finally' block?
            #
            shutil.rmtree(workdir, ignore_errors=True)

        except:
            dct[STATUS] = DONE
            dct[PRIORITY] = None
            raise

        if run_rval is INCOMPLETE:
            #mark the job as needing restarting
            dct[STATUS] = START
            dct[PRIORITY] = RESTART_PRIORITY
        else:
            #mark the job as being done
            dct[STATUS] = DONE
            dct[PRIORITY] = None

        break  # don't actually loop


