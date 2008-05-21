import threading, time, commands, os, sys, math, random

import psycopg2, psycopg2.extensions
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, MetaData, ForeignKey    
from sqlalchemy import Integer, String, Float, DateTime
from sqlalchemy.orm import mapper, relation, backref, eagerload


##########
#
# Connection stuff
#
#

if 0:
    _db_host = 'jais.iro.umontreal.ca'

    def _pwd():
        if not hasattr(_pwd,'rval'):
            pw_cmd = 'ssh bergstrj@grieg.iro.umontreal.ca cat .lisa_db'
            rval = commands.getoutput(pw_cmd)
        return rval

    def engine():
        """Create an engine to access lisa_db on gershwin

        This function caches the return value between calls.
        """
        if not hasattr(engine,'rval'):
            pw = _pwd()
            db_str ='postgres://bergstrj:%s@%s/lisa_db' % (pw,_db_host) 
            echo = False #spews pseudo-sql to stdout
            engine.rval = create_engine(db_str
                    ,pool_size=1 # should force the app release extra connections
                    # releasing connections should let us schedule more jobs, since each one operates 
                    # autonomously most of the time, just checking the db rarely.
                    # TODO: optimize this for large numbers of jobs
                    ,echo=echo
                    )
        return engine.rval

    def engine_serializable():
        """Create an engine to access lisa_db on gershwin, which uses serializable
        transaction mode.

        This function caches the return value between calls.
        """

        this = engine_serializable

        if not hasattr(this,'rval'):
            pw = _pwd()
            def connect():
                c = psycopg2.connect(user='bergstrj',
                        password=pw,
                        database='lisa_db',
                        host='gershwin.iro.umontreal.ca')
                c.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE)
                return c
            pool_size=0
            this.rval = create_engine('postgres://'
                    ,creator=connect
                    ,pool_size=0 # should force the app release connections
                    )
        return this.rval


    Session = sessionmaker(bind=engine(), autoflush=True, transactional=True)
    SessionSerial = sessionmaker(bind=engine_serializable(),
            autoflush=True, transactional=True)

else:
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    engine = create_engine('sqlite:///:memory:', echo=False)
    Session = sessionmaker(bind=engine, autoflush=True, transactional=True)
    SessionSerial = Session


################
#
# Database setup
#
table_prefix='bergstrj_scratch_test_'
metadata = MetaData()

def table_with_id(name, *args):
    return Table(name, metadata,
        Column('id', Integer, primary_key=True),
        *args)

t_trial = table_with_id(table_prefix+'trial',
        Column('desc', String(256)), #comment: why running this trial?
        Column('priority', Float(53)), #aka Double
        Column('start', DateTime),
        Column('finish', DateTime),
        Column('host', String(256)))

t_keyval = table_with_id(table_prefix+'keyval',
        Column('name', String(32), nullable=False), #name of attribute
        Column('fval', Float(53)), #aka Double
        Column('ival', Integer),
        Column('sval', String(256))) #TODO: store text (strings of unbounded length)

t_trial_keyval = table_with_id(table_prefix+'trial_keyval',
        Column('trial_id', Integer, ForeignKey('%s.id' % t_trial)),
        Column('keyval_id', Integer, ForeignKey('%s.id' % t_keyval)))

class _KeyVal(object):
    @staticmethod
    def cache(name, val, session, create=True):
        #TODO: consider using a local cache to remove the need to query db
        #      this takes advantage of common usage, which is to only add KeyVal
        #      pairs.
        #check if it is in the DB
        q = session.query(_KeyVal)
        if isinstance(val, float):
            q = q.filter_by(name=name, fval=val)
        elif isinstance(val, int):
            q = q.filter_by(name=name, ival=val)
        elif isinstance(val, str):
            q = q.filter_by(name=name, sval=val)
        else:
            raise TypeError(val)
        rval = q.first()
        if rval is None and create:
            rval = _KeyVal(name, val)
        session.save_or_update(rval)
        return rval

    def __init__(self, name, val):
        self.name = name
        self.val = val
    def __get_val(self):
        val = None
        if self.fval is not None: val = self.fval
        if self.ival is not None: val = self.ival
        if self.sval is not None: val = self.sval
        return  val
    def __set_val(self, val):
        if isinstance(val, float):
            self.fval = val
            self.ival = None
            self.sval = None
        elif isinstance(val, int):
            self.fval = None
            self.ival = val
            self.sval = None
        elif isinstance(val, str):
            self.fval = None
            self.ival = None
            self.sval = val
        else:
            raise TypeError(val)
    val = property(__get_val, __set_val)
    def __repr__(self):
        return "<Param(%s,'%s', %s)>" % (self.id, self.name, repr(self.val))
mapper(_KeyVal, t_keyval)


class Trial(object):
    _native_cols = 'desc', 'priority', 'start', 'finish', 'host'
    _forbidden_keynames = set(['filter', 'load', 'desc', 
            'priority', 'start', 'finish', 'host',
            'create', 'max_sleep', 'max_retry', 'session', 'c',
            'abort', 'complete'])

    class ReserveError(Exception): """reserve failed"""

    @staticmethod
    def filter(session=None, **kwargs):
        """Construct a query for Trials.

        The result is iterable, but you can also call .first() on it.

        """
        if session is None: 
            session = Session()
        q = session.query(Trial)
        for col in Trial._native_cols:
            if col in kwargs:
                q = q.filter_by(**{col:kwargs[col]})
                del kwargs[col]
        for kw, arg in kwargs.items():
            if isinstance(arg, float):
                q = q.filter(Trial._attrs.any(name=kw, fval=arg))
            elif isinstance(arg, int):
                q = q.filter(Trial._attrs.any(name=kw, ival=arg))
            elif isinstance(arg, str):
                q = q.filter(Trial._attrs.any(name=kw, sval=arg))
            else:
                raise TypeError(arg)
        return q

    @staticmethod
    def load(create=True, **kwargs):
        #is it weird that load would create one if not found?
        trial = Trial.filter(**kwargs).first()
        if trial is None and create:
            trial = Trial(**kwargs)
        return trial

    @staticmethod
    def load_reserve(max_retry=10, max_sleep=5.0, **kwargs):
        """Book a job.

        If no jobs remain, raise StopIteration
        
        @todo: is the jobtype mechanism necessary or sufficient?
        @todo: what about filtering based on the jobs (key,val) pairs? (pro: maybe more robust.  con: forces jobs to have (key,val) pairs)

        @jobtype: string identifier to filter potential jobs
        @param max_retry: try this many times to reserve a job before raising an exception
        @param max_sleep: sleep up to this many seconds between retry attempts

        @return job id
        """

        s = SessionSerial()
        retry = max_retry
        trial = None
        assert 'start' not in kwargs

        while (trial is None) and retry:
            #(re)build a query
            q = s.query(Trial).\
                    filter_by(start=None, **kwargs).\
                    order_by(Trial.c.priority)
            q = q.options(eagerload('_attrs')) #TODO is this a good idea?

            trial = q.first()
            if trial is None:
                return None # no jobs match the query
            else:
                try:
                    trial.reserve()
                except Trial.ReserveError, e:
                    s.rollback()
                    waittime = random.random() * max_sleep
                    if debug: print 'another process stole our trial. Waiting %f secs' % wait
                    time.sleep(waittime)
                    retry -= 1

        if trial: 
            s.expunge(trial)
        s.close()
        return trial

    def __init__(self, desc=None, priority=None, start=None, finish=None, host=None, **kwargs):
        self.desc = desc
        self.priority = priority
        self.start = start
        self.finish = finish
        self.host = host
        self.attrs.update(kwargs)
    def __repr__(self):
        return "<Trial(%s, '%s', %s, '%s', %s, %s)>" \
                %(str(self.id), self.desc, self.priority, self.host,
                        self.start, self.finish)

    def _get_attrs(self):
        class AttrCatcher(object):
            #TODO: make these methods faster with better data structures
            def __getattr__(attr_self, attr):
                attrlist = self._attrs
                for i,a in enumerate(attrlist):
                    if a.name == attr:
                        return a.val
                raise AttributeError(attr)
            def __setattr__(attr_self, attr, val):
                n = 0
                s = Session()
                assert attr not in Trial._forbidden_keynames
                for i,a in enumerate(self._attrs):
                    if a.name == attr:
                        attrlist[i] = _KeyVal.cache(attr,val, s)
                        n += 1
                assert n <= 1
                if n == 0:
                    self._attrs.append(_KeyVal.cache(attr,val, s))
                s.commit()
            def __iter__(_self):
                def gen():
                    #for a in self._attrs:
                    #    yield a.name
                    return self._attrs.__iter__()
                return gen()
            def update(attr_self, dct):
                for k,v in dct.items():
                    setattr(attr_self, k, v)
                            
        #we can't add attributes to self, so just do this...
        return AttrCatcher() #seriously, allocate a new one each time
    attrs = property(_get_attrs)
    def reserve(self,
            session=None,
            max_retry=10,
            max_sleep=4.0):
        """Reserve the job for the current process, to the exclusion of
        all other processes.  In other words, lock it."""
        if session is None:
            s = SessionSerial()
        else:
            s = session
        serial_self = s.query(Trial).get(self.id)
        if serial_self.start is not None:
            raise Trial.ReserveError(self.host)
        serial_self.start = datetime.datetime.now()
        serial_self.host = 'asdf' #TODO: get hostname
        try:
            s.commit()
        except (psycopg2.OperationalError,
                sqlalchemy.exceptions.ProgrammingError), e:
            #s.rollback() # doc says rollback or close after commit exception
            s.close()
            raise Trial.ReserveError(self.host)

        Session().refresh(self)

    def abort(self):
        """Reset job to be available for reservation.

        @return None

        @note: Raises exception if job is finished
        
        """
        if self.finish is not None:
            raise Exception('wtf?')
        self.start = None
        self.host = None
        Session().save(self)

    def complete(self, **kwargs):
        self.attrs.update(kwargs)
        self.finish = datetime.datetime.now()
        Session.save(self)

mapper(Trial, t_trial,
        properties = {
            '_attrs': relation(_KeyVal, 
                secondary=t_trial_keyval, 
                cascade="delete-orphan")
            })

metadata.create_all(engine) 




############
#
# Test Stuff
#
#

if __name__ == '__main__':
    s = Session()

    def add_some_jobs():
        dvalid, dtest = 'dvalid', 'dtest file'
        desc = 'debugging'

        def blah():
            for lr in [0.001, 0.01]:
                for scale in [0.0001 * math.sqrt(10.0)**i for i in range(4)]:
                    for rng_seed in [4, 5, 6]:
                        for priority in [None, 1, 2]:
                            yield locals()

        for kwargs in blah():
            t = Trial.load(desc=desc, dvalid=dvalid, dtest=dtest, **kwargs)
            s.save(t)

    def work(jobtype):
        try:
            jid = reserve(jobtype)
        except StopIteration:
            return

        def blah(*args):
            print 'blah: ', args
        dct = get_next()

    add_some_jobs()

    print 'hah'

    for t in s.query(Trial).order_by(Trial.c.priority):
        print t
        print list(t.attrs)



