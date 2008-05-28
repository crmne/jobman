
class DbHandle (object):
    """
    This class also provides filtering shortcuts that hide the names of the
    DbHandle internal databases.

    """

    DictIdTable = None
    """
    An SqlAlchemy-mapped class corresponding to database table with the
    following schema:

        Column('id', Integer, primary_key=True)
        Column('create_date', DateTime)
        Column('write_date', DateTime)
        Column('read_date', DateTime)
    
    """

    KeyValTable = None
    """
    An SqlAlchemy-mapped class corresponding to database table with the
    following schema:

        Column('id', Integer, primary_key=True)
        Column('name', String(128))
        Column('ival', Integer)
        Column('fval', Double)
        Column('cval', Complex)
        Column('sval', Text))
        Column('bval', Binary)  
    """

    MemberTable = None
    """
    An SqlAlchemy-mapped class corresponding to database table with the
    following schema:

        Column('id', Integer, primary_key=True)

        Column('trial_id', Integer, ForeignKey('%s.id' % t_trial)),
        Column('keyval_id', Integer, ForeignKey('%s.id' % t_keyval)))

        TODO: Consider making ('trial_id', 'keyval_id') a joint primary key
    """

    def insert(self, dct):
        """
        @rtype:  DbDict with reference to self
        @return: a DbDict initialized as a copy of dct
        
        @type dct: dict-like instance whose keys are strings, and values are
        either strings, integers, floats

        @param dct: dictionary to insert

        """
    def query(self, session): 
        """Construct an SqlAlchemy query, which can be subsequently filtered
        using the filtering classmethods of DbDict"""
        return session.Query(self.DictTable)
        pass

    def with_any(self, kw, arg):
        pass

    def with_cond(self, todo):
        pass

class DbDict (object):
    """
    Instances are dict-like objects with additional features for communicating
    with an active database handle.

    """

    def reload(): 
        """Sync key-value pairs from database to self"""
    def save():
        """Sync key-value pairs from self to database"""
        pass
    def delete(self):
        pass

