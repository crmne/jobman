from pylearn.dbdict.experiment import *
from unittest import TestCase

import StringIO


class T_subdict(TestCase):

    def test0(self):
        a = {'aa':0, 'ab':1, 'bb':2}
        s = subdict(a, 'a') # returns dict-like object with keyvals {'a':0, 'b':1}
        s['a'] = 5
        s['c'] = 9

        self.failUnless(s['c'] == 9)
        self.failUnless(a['ac'] == 9)

        #check that the subview has the right stuff
        sitems = s.items()
        sitems.sort()
        self.failUnless(sitems == [('a', 5), ('b', 1), ('c', 9)], str(sitems))
        self.failUnless(a['bb'] == 2)

        #add to the subview via the parent
        a['az'] = -1
        self.failUnless(s['z'] == -1)

    def test1(self):
        a = {'aa':0, 'ab':1, 'bb':2}

        s = subdict(a, 'a')

        r = {}
        r.update(s)

        self.failUnless(len(r) == len(s))
        self.failUnless(r == s, (str(r), str(s)))


class T_call_with_kwargs_from_dict(TestCase):

    def test0(self):

        def f(a, c=5):
            return a+c

        def g(a, **dct):
            return a + dct['c']


        kwargs = dict(a=1, b=2, c=3)

        io = StringIO.StringIO()

        self.failUnless(call_with_kwargs_from_dict(f, kwargs, logfile=io) == 4)
        self.failUnless(io.getvalue() == \
                "WARNING: DictProxyState.call_substate ignoring key-value pair: b 2\n")
        self.failUnless(call_with_kwargs_from_dict(g, kwargs, logfile=io) == 4)
        self.failUnless(io.getvalue() == \
                "WARNING: DictProxyState.call_substate ignoring key-value pair: b 2\n")
        del kwargs['c']
        self.failUnless(call_with_kwargs_from_dict(f, kwargs, logfile=io) == 6)
        self.failUnless(io.getvalue() ==  \
                ("WARNING: DictProxyState.call_substate ignoring key-value pair: b 2\n"
                "WARNING: DictProxyState.call_substate ignoring key-value pair: b 2\n"))

