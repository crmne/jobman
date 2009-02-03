
import sys, os, copy, time

import numpy.random

import sqlalchemy
import sqlalchemy.pool
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import eagerload
import psycopg2, psycopg2.extensions 

from api0 import db_from_engine, postgres_db, DbHandle


EXPERIMENT = 'dbdict.experiment'
#using the dictionary to store these is too slow
STATUS = 'dbdict.status'
HASH = 'dbdict.hash'
PRIORITY = 'dbdict.sql.priority'

HOST = 'dbdict.sql.hostname'
HOST_WORKDIR = 'dbdict.sql.host_workdir'
PUSH_ERROR = 'dbdict.sql.push_error'

START = 0
RUNNING = 1
DONE = 2
FUCKED_UP = 666

_TEST_CONCURRENCY = False

def postgres_serial(user, password, host, database, poolclass=sqlalchemy.pool.NullPool, **kwargs):
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
        this.engine = create_engine('postgres://'
                ,creator=connect
                ,poolclass=poolclass
                )

    db = db_from_engine(this.engine, **kwargs)
    db._is_serialized_session_db = True
    return db

def book_dct_postgres_serial(db, retry_max_sleep=10.0, verbose=0):
    """Find a trial in the lisa_db with status START.

    A trial will be returned with status=RUNNING.

    Returns None if no such trial exists in DB.

    This function uses a serial access to the lisadb to guarantee that no other
    process will retrieve the same dct.  It is designed to facilitate writing
    a "consumer" for a Producer-Consumer pattern based on the database.

    """
    print >> sys.stderr, """#TODO: use the priority field, not the status."""
    print >> sys.stderr, """#TODO: ignore entries with key PUSH_ERROR."""

    s = db.session() #open a new session

    # NB. we need the query and attribute update to be in the same transaction
    assert s.autocommit == False 

    dcts_seen = set([])
    keep_trying = True

    dct = None
    while (dct is None) and keep_trying:
        #build a query
        q = s.query(db._Dict)

        #N.B.
        # use dedicated column to retrieve jobs, not the dictionary keyval pair
        # This should be much faster.
        q = q.filter(db._Dict.status==START)
        q = q.order_by(db._Dict.priority.desc())

        # this doesn't seem to work, hene the string hack below
        q = q.options(eagerload('_attrs')) #hard-coded in api0

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
                sqlalchemy.exceptions.ProgrammingError,
                sqlalchemy.exc.DBAPIError), e:
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

    if dct:
        str(dct) # for loading of attrs in UGLY WAY!!!
    s.close()
    return dct

def book_dct_non_postgres(db):
    print >> sys.stderr, """#TODO: use the priority field, not the status."""
    print >> sys.stderr, """#TODO: ignore entries with key self.push_error."""

    raise NotImplementedError()


###########
# Connect
###########

def parse_dbstring(dbstring):
    postgres = 'postgres://'
    if not dbstring.startswith(postgres):
        raise ValueError('For now, dbdict dbstrings must start with postgres://', dbstring)
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
        password = get_password(hostname, dbname)

    if False:
        print 'USERNAME', username
        print 'PASS', password
        print 'HOST', hostname
        print 'DB', dbname
        print 'TABLE', tablename

    return username, password, hostname, dbname, tablename

def get_password(hostname, dbname):
    """Return the current user's password for a given database

    :TODO: Replace this mechanism with a section in the pylearn configuration file
    """
    password_path = os.getenv('HOME')+'/.dbdict_%s'%dbname
    try:
        password = open(password_path).readline()[:-1] #cut the trailing newline
    except:
        raise ValueError( 'Failed to read password for db "%s" from %s' % (dbname, password_path))
    return password

def db(dbstring):
    username, password, hostname, dbname, tablename = parse_dbstring(dbstring)
    try:
        return postgres_db(username, password, hostname, dbname, table_prefix=tablename)
    except:
        print 'Error connecting with password', password
        raise


###########
# Queue
###########

def insert_dict(jobdict, db, force_dup=False, session=None, priority=1.0):
    """Insert a new `job` dictionary into database `db`.

    :param force_dup: forces insertion even if an identical dictionary is already in the db

    """
    # compute hash for the job, will be used to avoid duplicates
    job = copy.copy(jobdict)
    jobhash = hash(`job`)

    if session is None:
        s = db.session()
    else:
        s = session

    do_insert = force_dup or (None is s.query(db._Dict).filter(db._Dict.hash==jobhash).filter(db._Dict.status!=FUCKED_UP).first())

    rval = None
    if do_insert:
        if STATUS not in job:
            job[STATUS] = START
        if HASH not in job:
            job[HASH] = jobhash
        if PRIORITY not in job:
            job[PRIORITY] = priority
        rval = db.insert(job, session=s)
        s.commit()

    if session is None:
        s.close()
    return rval


def insert_job(experiment_fn, state, db, force_dup=False, session=None, priority=1.0):
    state = copy.copy(state)
    experiment_name = experiment_fn.__module__ + '.' + experiment_fn.__name__
    if EXPERIMENT in state:
        if state[EXPERIMENT] != experiment_name:
            raise Exception('Inconsistency: state element %s does not match experiment %s' %(EXPERIMENT, experiment_name))
    else:
        state[EXPERIMENT] = experiment_name
    return insert_dict(state, db, force_dup=force_dup, session=session, priority=priority)


# TODO: FIXME: WARNING
# Should use insert_dict instead of db.insert.  Need one entry point for adding jobs to 
# database, so that hashing can be done consistently
def add_experiments_to_db(jobs, db, verbose=0, add_dups=False, type_check=None, session=None):
    """Add experiments paramatrized by jobs[i] to database db.

    Default behaviour is to ignore jobs which are already in the database.

    If type_check is a class (instead of None) then it will be used as a type declaration for
    all the elements in each job dictionary.  For each key,value pair in the dictionary, there
    must exist an attribute,value pair in the class meeting the following criteria:
    the attribute and the key are equal, and the types of the values are equal.

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
        if session is None:
            s = db.session()
            do_insert = force_dup or (None is db.query(s).filter_eq_dct(job).first())
            s.close()
        else:
            do_insert = force_dup or (None is db.query(session).filter_eq_dct(job).first())

        if do_insert:
            if type_check:
                for k,v in job.items():
                    if type(v) != getattr(type_check, k):
                        raise TypeError('Experiment contains value with wrong type',
                                ((k,v), getattr(type_check, k)))

            job[STATUS] = START
            job[PRIORITY] = 1.0
            if verbose:
                print 'ADDING  ', job
            db.insert(job)
            rval.append((True, job))
        else:
            if verbose:
                print 'SKIPPING', job
            rval.append((False, job))


