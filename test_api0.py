from api0 import *
import threading, time, commands, os, sys, math, random, datetime

import psycopg2, psycopg2.extensions
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, MetaData, ForeignKey    
from sqlalchemy import Integer, String, Float, DateTime, Text, Binary
from sqlalchemy.orm import mapper, relation, backref, eagerload
from sqlalchemy.sql import operators, select

import unittest

class T(unittest.TestCase):

    def test_bad_dict_table(self):
        """Make sure our crude version of schema checking kinda works"""
        engine = create_engine('sqlite:///:memory:', echo=False)
        Session = sessionmaker(bind=engine, autoflush=True, transactional=True)

        table_prefix='bergstrj_scratch_test_'
        metadata = MetaData()
        t_trial = Table(table_prefix+'trial', metadata,
                Column('id', Integer, primary_key=True),
                Column('desc', String(256)), #comment: why running this trial?
                Column('priority', Float(53)), #aka Double
                Column('start', DateTime),
                Column('finish', DateTime),
                Column('host', String(256)))
        metadata.create_all(engine)

        try:
            h = DbHandle(None, t_trial, None, None)
        except ValueError, e:
            if e[0] == DbHandle.e_bad_table:
                return
        self.fail()


    def go(self):
        """Create tables and session_maker"""
        engine = create_engine('sqlite:///:memory:', echo=False)
        Session = sessionmaker(autoflush=True, transactional=True)

        table_prefix='bergstrj_scratch_test_'
        metadata = MetaData()

        t_trial = Table(table_prefix+'trial', metadata,
                Column('id', Integer, primary_key=True),
                Column('create', DateTime),
                Column('write', DateTime),
                Column('read', DateTime))

        t_keyval = Table(table_prefix+'keyval', metadata,
                Column('id', Integer, primary_key=True),
                Column('name', String(32), nullable=False), #name of attribute
                Column('ntype', Integer),
                Column('fval', Float(53)), #aka Double
                Column('sval', Text), #aka Double
                Column('bval', Binary)) #TODO: store text (strings of unbounded length)

        t_trial_keyval = Table(table_prefix+'trial_keyval', metadata,
                Column('dict_id', Integer, ForeignKey('%s.id' % t_trial),
                    primary_key=True),
                Column('pair_id', Integer, ForeignKey('%s.id' % t_keyval),
                    primary_key=True))

        metadata.bind = engine
        metadata.create_all() # does nothing when tables already exist
        
        self.engine = engine
        return Session, t_trial, t_keyval, t_trial_keyval


    def test_insert_save(self):

        Session, t_dict, t_pair, t_link = self.go()

        db = DbHandle(*self.go())

        def jobs():
            dvalid, dtest = 'dvalid', 'dtest file'
            desc = 'debugging'
            for lr in [0.001]:
                for scale in [0.0001 * math.sqrt(10.0)**i for i in range(4)]:
                    for rng_seed in [4, 5, 6]:
                        for priority in [None, 1]:
                            yield dict(locals())

        jlist = list(jobs())
        assert len(jlist) == 1*4*3*2
        for i, dct in enumerate(jobs()):
            t = db.insert(**dct)

        #make sure that they really got inserted into the db
        orig_keycount = db._session.query(db._KeyVal).count()
        self.failUnless(orig_keycount > 0, orig_keycount)

        orig_dctcount = Session().query(db._Dict).count()
        self.failUnless(orig_dctcount ==len(jlist), orig_dctcount)

        orig_keycount = Session().query(db._KeyVal).count()
        self.failUnless(orig_keycount > 0, orig_keycount)

        #queries
        q0list = list(db.query().all())
        q1list = list(db.query())
        q2list = list(db)

        self.failUnless(q0list == q1list, (q0list,q1list))
        self.failUnless(q0list == q2list, (q0list,q1list))

        self.failUnless(len(q0list) == len(jlist))

        for i, (j, q) in enumerate(zip(jlist, q0list)):
            jitems = list(j.items())
            qitems = list(q.items())
            jitems.sort()
            qitems.sort()
            if jitems != qitems:
                print i
                print jitems
                print qitems
            self.failUnless(jitems == qitems, (jitems, qitems))

    def test_query_0(self):
        Session, t_dict, t_pair, t_link = self.go()

        db = DbHandle(*self.go())

        def jobs():
            dvalid, dtest = 'dvalid', 'dtest file'
            desc = 'debugging'
            for lr in [0.001]:
                for scale in [0.0001 * math.sqrt(10.0)**i for i in range(4)]:
                    for rng_seed in [4, 5, 6]:
                        for priority in [None, 1]:
                            yield dict(locals())

        jlist = list(jobs())
        assert len(jlist) == 1*4*3*2
        for i, dct in enumerate(jobs()):
            t = db.insert(**dct)

        qlist = list(db.query(rng_seed=5))
        self.failUnless(len(qlist) == len(jlist)/3)

        jlist5 = [j for j in jlist if j['rng_seed'] == 5]

        for i, (j, q) in enumerate(zip(jlist5, qlist)):
            jitems = list(j.items())
            qitems = list(q.items())
            jitems.sort()
            qitems.sort()
            if jitems != qitems:
                print i
                print jitems
                print qitems
            self.failUnless(jitems == qitems, (jitems, qitems))

    def test_delete_keywise(self):
        Session, t_dict, t_pair, t_link = self.go()

        db = DbHandle(*self.go())

        def jobs():
            dvalid, dtest = 'dvalid', 'dtest file'
            desc = 'debugging'
            for lr in [0.001]:
                for scale in [0.0001 * math.sqrt(10.0)**i for i in range(4)]:
                    for rng_seed in [4, 5, 6]:
                        for priority in [None, 1]:
                            yield dict(locals())

        jlist = list(jobs())
        assert len(jlist) == 1*4*3*2
        for i, dct in enumerate(jobs()):
            t = db.insert(**dct)

        orig_keycount = Session().query(db._KeyVal).count()

        del_count = Session().query(db._KeyVal).filter_by(name='rng_seed',
                fval=5.0).count()
        self.failUnless(del_count == 8, del_count)

        #delete all the rng_seed = 5 entries
        qlist_before = list(db.query(rng_seed=5))
        for q in qlist_before:
            del q['rng_seed']

        #check that it's gone from our objects
        for q in qlist_before:
            self.failUnless('rng_seed' not in q)  #via __contains__
            self.failUnless('rng_seed' not in q.keys()) #via keys()
            exc=None
            try:
                r = q['rng_seed'] # via __getitem__
                print 'r,', r
            except KeyError, e:
                pass

        #check that it's gone from dictionaries in the database
        qlist_after = list(db.query(rng_seed=5))
        self.failUnless(qlist_after == [])

        #check that exactly 8 keys were removed
        new_keycount = Session().query(db._KeyVal).count()
        self.failUnless(orig_keycount == new_keycount + 8, (orig_keycount,
            new_keycount))

        #check that no keys have rng_seed == 5
        gone_count = Session().query(db._KeyVal).filter_by(name='rng_seed',
                fval=5.0).count()
        self.failUnless(gone_count == 0, gone_count)


    def test_delete_dictwise(self):
        Session, t_dict, t_pair, t_link = self.go()

        db = DbHandle(*self.go())

        def jobs():
            dvalid, dtest = 'dvalid', 'dtest file'
            desc = 'debugging'
            for lr in [0.001]:
                for scale in [0.0001 * math.sqrt(10.0)**i for i in range(4)]:
                    for rng_seed in [4, 5, 6]:
                        for priority in [None, 1]:
                            yield dict(locals())

        jlist = list(jobs())
        assert len(jlist) == 1*4*3*2
        for i, dct in enumerate(jobs()):
            t = db.insert(**dct)

        orig_keycount = Session().query(db._KeyVal).count()
        orig_dctcount = Session().query(db._Dict).count()
        self.failUnless(orig_dctcount == len(jlist))

        #delete all the rng_seed = 5 dictionaries
        qlist_before = list(db.query(rng_seed=5))
        for q in qlist_before:
            q.delete()

        #check that the right number has been removed
        post_dctcount = Session().query(db._Dict).count()
        self.failUnless(post_dctcount == len(jlist)-8)

        #check that the remaining ones are correct
        for a, b, in zip(
                [j for j in jlist if j['rng_seed'] != 5],
                Session().query(db._Dict).all()):
            self.failUnless(a == b)

        #check that the keys have all been removed
        n_keys_per_dict = 8
        new_keycount = Session().query(db._KeyVal).count()
        self.failUnless(orig_keycount - 8 * n_keys_per_dict == new_keycount, (orig_keycount,
            new_keycount))


    def test_setitem_0(self):
        Session, t_dict, t_pair, t_link = self.go()

        db = DbHandle(*self.go())

        b0 = 6.0
        b1 = 9.0

        job = dict(a=0, b=b0, c='hello')

        dbjob = db.insert(**job)

        dbjob['b'] = b1

        #check that the change is in db
        qjob = Session().query(db._Dict).filter(db._Dict._attrs.any(name='b',
            fval=b1)).first()
        self.failIf(qjob is dbjob)
        self.failUnless(qjob == dbjob)

        #check that the b:b0 key is gone
        count = Session().query(db._KeyVal).filter_by(name='b', fval=b0).count()
        self.failUnless(count == 0, count)

        #check that the b:b1 key is there
        count = Session().query(db._KeyVal).filter_by(name='b', fval=b1).count()
        self.failUnless(count == 1, count)

    def test_setitem_1(self):
        """replace with different sql type"""
        Session, t_dict, t_pair, t_link = self.go()

        db = DbHandle(*self.go())

        b0 = 6.0
        b1 = 'asdf' # a different dtype

        job = dict(a=0, b=b0, c='hello')

        dbjob = db.insert(**job)

        dbjob['b'] = b1

        #check that the change is in db
        qjob = Session().query(db._Dict).filter(db._Dict._attrs.any(name='b',
            sval=b1)).first()
        self.failIf(qjob is dbjob)
        self.failUnless(qjob == dbjob)

        #check that the b:b0 key is gone
        count = Session().query(db._KeyVal).filter_by(name='b', fval=b0).count()
        self.failUnless(count == 0, count)

        #check that the b:b1 key is there
        count = Session().query(db._KeyVal).filter_by(name='b', sval=b1,
                fval=None).count()
        self.failUnless(count == 1, count)

    def test_setitem_2(self):
        """replace with different number type"""
        Session, t_dict, t_pair, t_link = self.go()

        db = DbHandle(*self.go())

        b0 = 6.0
        b1 = 7

        job = dict(a=0, b=b0, c='hello')

        dbjob = db.insert(**job)

        dbjob['b'] = b1

        #check that the change is in db
        qjob = Session().query(db._Dict).filter(db._Dict._attrs.any(name='b',
            fval=b1)).first()
        self.failIf(qjob is dbjob)
        self.failUnless(qjob == dbjob)

        #check that the b:b0 key is gone
        count = Session().query(db._KeyVal).filter_by(name='b', fval=b0,ntype=1).count()
        self.failUnless(count == 0, count)

        #check that the b:b1 key is there
        count = Session().query(db._KeyVal).filter_by(name='b', fval=b1,ntype=0).count()
        self.failUnless(count == 1, count)
        

if __name__ == '__main__':
    unittest.main()
