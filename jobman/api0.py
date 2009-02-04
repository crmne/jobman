from sqlalchemy import create_engine, desc
import sqlalchemy.pool
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, MetaData, ForeignKey, ForeignKeyConstraint
from sqlalchemy import Integer, String, Float, Boolean, DateTime, Text, Binary
from sqlalchemy.databases import postgres
from sqlalchemy.orm import mapper, relation, backref, eagerload
from sqlalchemy.sql import operators, select
from sql_commands import crazy_sql_command

class Todo(Exception): """Replace this with some working code!"""

class DbHandle (object):
    """
    This class also provides filtering shortcuts that hide the names of the
    DbHandle internal databases.

    Attributes:
    dict_table
    pair_table



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

    """

    e_bad_table = 'incompatible columns in table'

    def __init__(h_self, Session, engine, dict_table, pair_table):
        h_self._engine = engine;
        h_self._dict_table = dict_table
        h_self._pair_table = pair_table

        #TODO: replace this crude algorithm (ticket #17)
        if ['id', 'create', 'write', 'read', 'status', 'priority','hash'] != [c.name for c in dict_table.c]:
            raise ValueError(h_self.e_bad_table, dict_table)
        if ['id', 'dict_id', 'name', 'ntype', 'fval', 'sval', 'bval'] != [c.name for c in pair_table.c]:
            raise ValueError(h_self.e_bad_table, pair_table)

        h_self._session_fn = Session

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
            def __init__(d_self, session=None):
                if session is None:
                    s = h_self._session_fn()
                    s.add(d_self) #d_self transient -> pending
                    s.commit()    #d_self -> persistent
                    s.close()     #d_self -> detached
                else:
                    s = session
                    s.save(d_self)

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

            def __setitem__(d_self, key, val, session=None):
                if session is None:
                    s = h_self._session_fn()
                    s.add(d_self)
                    d_self._set_in_session(key, val, s)
                    s.commit()
                    s.close()
                else:
                    s = session
                    s.add(d_self)
                    d_self._set_in_session(key, val, s)

            def __delitem__(d_self, key, session=None):
                if session is None:
                    s = h_self._session_fn()
                    commit_close = True
                else:
                    s = session
                    commit_close = False
                s.add(d_self)

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
                if commit_close:
                    s.commit()
                    s.close()

            def iteritems(d_self):
                return d_self.items()

            def items(d_self):
                return [(kv.name, kv.val) for kv in d_self._attrs]
            
            def keys(d_self):
                return [kv.name for kv in d_self._attrs]

            def values(d_self):
                return [kv.val for kv in d_self._attrs]

            def update(d_self, dct, session=None, **kwargs):
                """Like dict.update(), set keys from kwargs"""
                if session is None:
                    s = h_self._session_fn()
                    commit_close = True
                else:
                    s = session
                    commit_close = False
                s.add(d_self)
                for k, v in dct.items():
                    d_self._set_in_session(k, v, s)
                for k, v in kwargs.items():
                    d_self._set_in_session(k, v, s)

                if commit_close:
                    s.commit()
                    s.close()

            def get(d_self, key, default):
                try:
                    return d_self[key]
                except KeyError:
                    return default

            def __str__(self):
                return 'Dict'+ str(dict(self))

            #
            # database stuff
            #

            def refresh(d_self, session=None): 
                """Sync key-value pairs from database to self

                @param session: use the given session, and do not commit.
                
                """
                if session is None:
                    session = h_self._session_fn()
                    session.add(d_self) #so session knows about us
                    session.refresh(d_self)
                    session.commit()
                    session.close()
                else:
                    session.add(d_self) #so session knows about us
                    session.refresh(self.dbrow)

            def delete(d_self, session=None):
                """Delete this dictionary from the database
                
                @param session: use the given session, and do not commit.
                """
                if session is None:
                    session = h_self._session_fn()
                    session.add(d_self) #so session knows about us
                    session.delete(d_self) #mark for deletion
                    session.commit()
                    session.close()
                else:
                    session.add(d_self) #so session knows about us
                    session.delete(d_self)

            # helper routine by update() and __setitem__
            def _set_in_session(d_self, key, val, session):
                """Modify an existing key or create a key to hold val"""
                
                #FIRST SOME MIRRORING HACKS
                if key == 'dbdict.status':
                    ival = int(val)
                    d_self.status = ival
                if key == 'dbdict.sql.priority':
                    fval = float(val)
                    d_self.priority = fval
                if key == 'dbdict.hash':
                    ival = int(val)
                    d_self.hash = ival

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

            def filter_eq(q_self, kw, arg):
                """Return a Query object that restricts to dictionaries containing
                the given kwargs"""

                #Note: when we add new types to the key columns, add them here
                q = q_self._query
                T = h_self._Dict
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

            def filter_eq_dct(q_self, dct):
                rval = q_self
                for key, val in dct.items():
                    rval = rval.filter_eq(key,val)
                return rval

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
        s = h_self.session()
        rval = list(h_self.query(s).__iter__())
        s.close()
        return rval.__iter__()

    def insert_kwargs(h_self, session=None, **dct):
        """
        @rtype:  DbHandle with reference to self
        @return: a DbHandle initialized as a copy of dct
        
        @type dct: dict-like instance whose keys are strings, and values are
        either strings, integers, floats

        @param dct: dictionary to insert

        """
        return h_self.insert(dct, session=session)

    def insert(h_self, dct, session=None):
        """
        @rtype:  DbHandle with reference to self
        @return: a DbHandle initialized as a copy of dct
        
        @type dct: dict-like instance whose keys are strings, and values are
        either strings, integers, floats

        @param dct: dictionary to insert

        """
        if session is None:
            s = h_self.session()
            rval = h_self._Dict(s)
            if dct: rval.update(dct, session=s)
            s.commit()
            s.close()
        else:
            rval = h_self._Dict(session)
            if dct: rval.update(dct, session=session)
        return rval

    def query(h_self, session):
        """Construct an SqlAlchemy query, which can be subsequently filtered
        using the instance methods of DbQuery"""
        return h_self._Query(session.query(h_self._Dict)\
                        .options(eagerload('_attrs')))

    def createView(h_self, view):

        s = h_self.session()
        cols = []
        
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
                                    h_self._dict_table.name, \
                                    h_self._pair_table.name)
        
        print 'Creating sql view with command:\n', viewsql;
        h_self._engine.execute(viewsql);
        s.commit();
        s.close()

        class MappedClass(object):
            pass

        mapper(MappedClass, view)

        return MappedClass

    def session(h_self):
        return h_self._session_fn()

    def get(h_self, id):
        s = h_self.session()
        rval = s.query(h_self._Dict).get(id)
        if rval:
            #eagerload hack
            str(rval)
            rval.id
        s.close()
        return rval


        

def db_from_engine(engine, 
        table_prefix='DbHandle_default_',
        trial_suffix='trial',
        keyval_suffix='keyval'):
    """Create a DbHandle instance

    @type engine: sqlalchemy engine (e.g. from create_engine)
    @param engine: connect to this database for transactions

    @type table_prefix: string
    @type trial_suffix: string
    @type keyval_suffix: string

    @rtype: DbHandle instance

    @note: The returned DbHandle will use three tables to implement the
    many-to-many pattern that it needs: 
     - I{table_prefix + trial_suffix},
     - I{table_prefix + keyval_suffix}

    """
    Session = sessionmaker(autoflush=True, autocommit=False)

    metadata = MetaData()

    t_trial = Table(table_prefix+trial_suffix, metadata,
            Column('id', Integer, primary_key=True),
            Column('create', DateTime),
            Column('write', DateTime),
            Column('read', DateTime),
            Column('status', Integer),
            Column('priority', Float(53)),
            Column('hash', postgres.PGBigInteger))

    t_keyval = Table(table_prefix+keyval_suffix, metadata,
            Column('id', Integer, primary_key=True),
            Column('dict_id', Integer, index=True),
            Column('name', String(128), index=True, nullable=False), #name of attribute
            Column('ntype', Boolean),
            Column('fval', Float(53)),
            Column('sval', Text),
            Column('bval', Binary),
            ForeignKeyConstraint(['dict_id'], [table_prefix+trial_suffix+'.id']))

                #, ForeignKey('%s.id' % t_trial)),
    metadata.bind = engine
    metadata.create_all() # no-op when tables already exist
    #warning: tables can exist, but have incorrect schema
    # see bug mentioned in DbHandle constructor

    return DbHandle(Session, engine, t_trial, t_keyval)

def sqlite_memory_db(echo=False, **kwargs):
    """Return a DbHandle backed by a memory-based database"""
    engine = create_engine('sqlite:///:memory:', echo=False)
    return db_from_engine(engine, **kwargs)

def sqlite_file_db(filename, echo=False, **kwargs):
    """Return a DbHandle backed by a file-based database"""
    engine = create_engine('sqlite:///%s' % filename, echo=False)
    return db_from_engine(engine, **kwargs)

def postgres_db(user, password, host, database, echo=False, poolclass=sqlalchemy.pool.NullPool, **kwargs):
    """Create an engine to access a postgres_dbhandle
    """
    db_str ='postgres://%(user)s:%(password)s@%(host)s/%(database)s' % locals()

    engine = create_engine(db_str, echo=echo, poolclass=poolclass)

    return db_from_engine(engine, **kwargs)
