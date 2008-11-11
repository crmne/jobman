from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, MetaData, ForeignKey    
from sqlalchemy import Integer, String, Float, Boolean, DateTime, Text, Binary
from sqlalchemy.orm import mapper, relation, backref, eagerload
from sqlalchemy.sql import operators, select
from sql_commands import crazy_sql_command
import psycopg2, psycopg2.extensions

class Todo(Exception): """Replace this with some working code!"""

class DbHandle (object):
    """
    This class also provides filtering shortcuts that hide the names of the
    DbHandle internal databases.

    Attributes:
    dict_table
    pair_table
    link_table



    dict_table

        An SqlAlchemy-mapped class corresponding to database table with the
        following schema:

            Column('id', Integer, primary_key=True)
            Column('create', DateTime)
            Column('write', DateTime)
            Column('read', DateTime)

            #TODO: reconsider create/read/write

    pair_table

        An SqlAlchemy-mapped class corresponding to database table with the
        following schema:

            Column('id', Integer, primary_key=True)
            Column('name', String(128))
            Column('ntype', Boolean)
            Column('fval', Double)
            Column('sval', Text))
            Column('bval', Blob)  

            #TODO: Consider difference between text and binary
            #TODO: Consider adding a 'type' column
            #TODO: Consider union?
            #TODO: Are there stanard ways of doing this kind of thing?

    link_table

        An SqlAlchemy-mapped class corresponding to database table with the
        following schema:

            Column('dict_id', Integer, ForeignKey('%s.id' % t_trial), primary_key=True),
            Column('keyval_id', Integer, ForeignKey('%s.id' % t_keyval), primary_key=True))

    """

    e_bad_table = 'incompatible columns in table'

    def __init__(h_self, Session, engine, dict_table, pair_table, link_table):
        h_self._engine = engine;
        h_self._dict_table = dict_table
        h_self._pair_table = pair_table
        h_self._link_table = link_table

        #TODO: replace this crude algorithm (ticket #17)
        if ['id', 'create', 'write', 'read'] != [c.name for c in dict_table.c]:
            raise ValueError(h_self.e_bad_table, dict_table)
        if ['id', 'name', 'ntype', 'fval', 'sval', 'bval'] != [c.name for c in pair_table.c]:
            raise ValueError(h_self.e_bad_table, pair_table)
        if ['dict_id', 'pair_id'] != [c.name for c in link_table.c]:
            raise ValueError(h_self.e_bad_table, pair_table)

        h_self._session_fn = Session
        h_self._session = Session()

        class KeyVal (object):
            """KeyVal interfaces between python types and the database.

            It encapsulates heuristics for type conversion.
            """
            def __init__(k_self, name, val):
                k_self.name = name
                k_self.val = val
            def __repr__(k_self):
                return "<Param(%s,'%s', %s)>" % (k_self.id, k_self.name, repr(k_self.val))
            def __get_val(k_self):
                val = None
                if k_self.fval is not None: val = [int, float][k_self.ntype](k_self.fval)
                if k_self.bval is not None: val = eval(str(k_self.bval))
                if k_self.sval is not None: val = k_self.sval
                return  val
            def __set_val(k_self, val):
                if isinstance(val, (str,unicode)):
                    k_self.fval = None
                    k_self.bval = None
                    k_self.sval = val
                else:
                    k_self.sval = None
                    try:
                        f = float(val)
                    except (TypeError, ValueError):
                        f = None
                    if f is None: #binary data
                        k_self.bval = repr(val)
                        assert eval(k_self.bval) == val
                        k_self.fval = None
                        k_self.ntype = None
                    else:
                        k_self.bval = None
                        k_self.fval = f
                        k_self.ntype = isinstance(val,float)
            val = property(__get_val, __set_val)

        mapper(KeyVal, pair_table)
        
        class Dict (object):
            """
            Instances are dict-like objects with additional features for
            communicating with an active database.

            This class will be mapped by SqlAlchemy to the dict_table.

            Attributes:
            handle - reference to L{DbHandle} (creator)

            """
            def __init__(d_self):
                s = h_self._session
                s.save(d_self)
                s.commit()

            _forbidden_keys = set(['session'])

            #
            # dictionary interface
            #

            def __contains__(d_self, key):
                for a in d_self._attrs:
                    if a.name == key:
                        return True
                return False

            def __eq__(self, other):
                return dict(self) == dict(other)
            def __neq__(self, other):
                return dict(self) != dict(other)

            def __getitem__(d_self, key):
                for a in d_self._attrs:
                    if a.name == key:
                        return a.val
                raise KeyError(key)

            def __setitem__(d_self, key, val):
                s = h_self._session
                d_self._set_in_session(key, val, s)
                s.update(d_self)  #session update, not dict-like update
                s.commit()

            def __delitem__(d_self, key):
                s = h_self._session
                #find the item to delete in d_self._attrs
                to_del = None
                for i,a in enumerate(d_self._attrs):
                    if a.name == key:
                        assert to_del is None
                        to_del = (i,a)
                if to_del is None:
                    raise KeyError(key)
                else:
                    i, a = to_del
                    s.delete(a)
                    del d_self._attrs[i]
                s.commit()
                s.update(d_self)

            def items(d_self):
                return [(kv.name, kv.val) for kv in d_self._attrs]
            
            def keys(d_self):
                return [kv.name for kv in d_self._attrs]

            def values(d_self):
                return [kv.val for kv in d_self._attrs]

            def update(d_self, dct, **kwargs):
                """Like dict.update(), set keys from kwargs"""
                s = h_self._session
                for k, v in dct.items():
                    d_self._set_in_session(k, v, s)
                for k, v in kwargs.items():
                    d_self._set_in_session(k, v, s)
                s.update(d_self)
                s.commit()

            def get(d_self, key, default):
                try:
                    return d_self[key]
                except KeyError:
                    return default

            #
            # database stuff
            #

            def refresh(d_self, session=None): 
                """Sync key-value pairs from database to self

                @param session: use the given session, and do not commit.
                
                """
                if session is None:
                    session = h_self._session
                    session.refresh(d_self)
                    session.commit()
                else:
                    session.refresh(self.dbrow)

            def delete(d_self, session=None):
                """Delete this dictionary from the database
                
                @param session: use the given session, and do not commit.
                """
                if session is None:
                    session = h_self._session
                    session.delete(d_self)
                    session.commit()
                else:
                    session.delete(d_self)

            # helper routine by update() and __setitem__
            def _set_in_session(d_self, key, val, session):
                """Modify an existing key or create a key to hold val"""
                if key in d_self._forbidden_keys:
                    raise KeyError(key)
                created = None
                for i,a in enumerate(d_self._attrs):
                    if a.name == key:
                        assert created == None
                        created = h_self._KeyVal(key, val)
                        d_self._attrs[i] = created
                if not created:
                    created = h_self._KeyVal(key, val)
                    d_self._attrs.append(created)
                session.save(created)

        mapper(Dict, dict_table,
                properties = {
                    '_attrs': relation(KeyVal, 
                        secondary=link_table, 
                        cascade="all, delete-orphan")
                    })

        class _Query (object):
            """
            Attributes:
            _query - SqlAlchemy.Query object
            """

            def __init__(q_self, query):
                q_self._query = query

            def __iter__(q_self):
                return q_self.all().__iter__()

            def __getitem__(q_self, item):
                return q_self._query.__getitem__(item)

            def filter_by(q_self, **kwargs):
                """Return a Query object that restricts to dictionaries containing
                the given kwargs"""

                #Note: when we add new types to the key columns, add them here
                q = q_self._query
                T = h_self._Dict
                for kw, arg in kwargs.items():
                    if isinstance(arg, (str,unicode)):
                        q = q.filter(T._attrs.any(name=kw, sval=arg))
                    else:
                        try:
                            f = float(arg)
                        except (TypeError, ValueError):
                            f = None
                        if f is None:
                            q = q.filter(T._attrs.any(name=kw, bval=repr(arg)))
                        else:
                            q = q.filter(T._attrs.any(name=kw, fval=f))

                return h_self._Query(q)

            def all(q_self):
                """Return an iterator over all matching dictionaries.

                See L{SqlAlchemy.Query}
                """
                return q_self._query.all()

            def count(q_self):
                """Return the number of matching dictionaries.

                See L{SqlAlchemy.Query}
                """
                return q_self._query.count()

            def first(q_self):
                """Return some matching dictionary, or None
                See L{SqlAlchemy.Query}
                """
                return q_self._query.first()

            def all_ordered_by(q_self, key, desc=False):
                """Return query results, sorted.

                @type key: string or tuple of string or list of string
                @param: keys by which to sort the results.

                @rtype: list of L{DbHandle._Dict} instances
                @return: query results, sorted by given keys
                """

                # order_by is not easy to do in SQL based on the data structures we're
                # using.  Considering we support different data types, it may not be
                # possible at all.
                #
                # It would be easy if 'pivot' or 'crosstab' were provided as part of the
                # underlying API, but they are not. For example, read this:
                # http://www.simple-talk.com/sql/t-sql-programming/creating-cross-tab-queries-and-pivot-tables-in-sql/

                # load query results
                results = list(q_self.all())

                if isinstance(key, (tuple, list)):
                    val_results = [([d[k] for k in key], d) for d in results]
                else:
                    val_results = [(d[key], d) for d in results]

                val_results.sort() #interesting: there is an optional key parameter
                if desc:
                    val_results.reverse()
                return [vr[-1] for vr in val_results]

        h_self._KeyVal = KeyVal
        h_self._Dict = Dict
        h_self._Query = _Query

    def __iter__(h_self):
        return h_self.query().__iter__()

    def insert_kwargs(h_self, **dct):
        """
        @rtype:  DbHandle with reference to self
        @return: a DbHandle initialized as a copy of dct
        
        @type dct: dict-like instance whose keys are strings, and values are
        either strings, integers, floats

        @param dct: dictionary to insert

        """
        rval = h_self._Dict()
        if dct: rval.update(dct)
        return rval
    def insert(h_self, dct):
        """
        @rtype:  DbHandle with reference to self
        @return: a DbHandle initialized as a copy of dct
        
        @type dct: dict-like instance whose keys are strings, and values are
        either strings, integers, floats

        @param dct: dictionary to insert

        """
        rval = h_self._Dict()
        if dct: rval.update(dct)
        return rval

    def query(h_self, **kwargs): 
        """Construct an SqlAlchemy query, which can be subsequently filtered
        using the instance methods of DbQuery"""

        return h_self._Query(h_self._session.query(h_self._Dict)\
                        .options(eagerload('_attrs')))\
                        .filter_by(**kwargs)

    def createView(h_self, view):

        s = h_self._session;
        cols = [];
        
        for col in view.columns:
            if col.name is "id":
                continue;
            elif isinstance(col.type, (Integer,Float)):
                cols.append([col.name,'fval']);
            elif isinstance(col.type,String):
                cols.append([col.name,'sval']);
            elif isinstance(col.type,Binary):
                cols.append([col.name,'bval']);
            else:
                assert "Error: wrong column type in view",view.name;
        
        # generate raw sql command string
        viewsql = crazy_sql_command(view.name, cols, \
                                    h_self._pair_table.name, \
                                    h_self._link_table.name);
        
        #print 'Creating sql view with command:\n', viewsql;

        h_self._engine.execute(viewsql);
        s.commit();

        class MappedClass(object):
            pass

        mapper(MappedClass, view)

        return MappedClass

    def session(h_self):
        return h_self._session_fn()
        

def db_from_engine(engine, 
        table_prefix='DbHandle_default_',
        trial_suffix='trial',
        keyval_suffix='keyval',
        link_suffix='link'):
    """Create a DbHandle instance

    @type engine: sqlalchemy engine (e.g. from create_engine)
    @param engine: connect to this database for transactions

    @type table_prefix: string
    @type trial_suffix: string
    @type keyval_suffix: string
    @type link_suffix: string

    @rtype: DbHandle instance

    @note: The returned DbHandle will use three tables to implement the
    many-to-many pattern that it needs: 
     - I{table_prefix + trial_suffix},
     - I{table_prefix + keyval_suffix}
     - I{table_prefix + link_suffix}

    """
    Session = sessionmaker(autoflush=True, autocommit=False)

    metadata = MetaData()

    t_trial = Table(table_prefix+trial_suffix, metadata,
            Column('id', Integer, primary_key=True),
            Column('create', DateTime),
            Column('write', DateTime),
            Column('read', DateTime))

    t_keyval = Table(table_prefix+keyval_suffix, metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(128), nullable=False), #name of attribute
            Column('ntype', Boolean),
            Column('fval', Float(53)),
            Column('sval', Text),
            Column('bval', Binary))

    t_link = Table(table_prefix+link_suffix, metadata,
            Column('dict_id', Integer, ForeignKey('%s.id' % t_trial),
                primary_key=True),
            Column('pair_id', Integer, ForeignKey('%s.id' % t_keyval),
                primary_key=True))

    metadata.bind = engine
    metadata.create_all() # no-op when tables already exist
    #warning: tables can exist, but have incorrect schema
    # see bug mentioned in DbHandle constructor

    return DbHandle(Session, engine, t_trial, t_keyval, t_link)

def sqlite_memory_db(echo=False, **kwargs):
    """Return a DbHandle backed by a memory-based database"""
    engine = create_engine('sqlite:///:memory:', echo=False)
    return db_from_engine(engine, **kwargs)

def sqlite_file_db(filename, echo=False, **kwargs):
    """Return a DbHandle backed by a file-based database"""
    engine = create_engine('sqlite:///%s' % filename, echo=False)
    return db_from_engine(engine, **kwargs)

_db_host = 'jais.iro.umontreal.ca'
_pwd='potatomasher';

def postgres_db(echo=False, **kwargs):
    """Create an engine to access lisa_db on gershwin
    This function caches the return value between calls.
    """
    db_str ='postgres://dumi:%s@%s/lisa_db' % (_pwd,_db_host)

    # should force the app release extra connections releasing
    # connections should let us schedule more jobs, since each one
    # operates autonomously most of the time, just checking the db
    # rarely. TODO: optimize this for large numbers of jobs
    pool_size = 0;
    engine = create_engine(db_str, pool_size=pool_size, echo=echo)

    return db_from_engine(engine, **kwargs)

def postgres_serial(user, password, database, host, echo=False, **kwargs):
    """
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

    return db_from_engine(this.engine, **kwargs)

