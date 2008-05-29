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
        engine = create_engine('sqlite:///:memory:', echo=False)
        Session = sessionmaker(bind=engine, autoflush=True, transactional=True)

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

        metadata.create_all(engine) # does nothing when tables already exist
        
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

if __name__ == '__main__':
    unittest.main()
