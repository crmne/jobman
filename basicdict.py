import sys
#
#
# Utility
#
#

class BasicDict (object):
    """A class to store experiment configurations.

    Configuration variables are stored in class instance __dict__.  This class
    ensures that keys are alphanumeric strings, and that values can be rebuilt
    from their representations.  
    
    For now, values must be either numbers or strings.  
    @todo: add support for binary fields.

    It can be serialized to/from a python file.
    
    """
    def __init__(self, __dict__=None, **kwargs):
        if __dict__:
            for k, v in __dict__.items():
                self[k] = v
        if kwargs:
            for k, v in kwargs.items():
                self[k] = v

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        BasicDict.__checkkeyval__(key, value)
        self.__dict__[key] = value

    def __setattr__(self, key, value):
        self[key] = value #__setitem__

    def __hash__(self):
        """Compute a hash string from a dict, based on items().

        @type dct: dict of hashable keys and values.
        @param dct: compute the hash of this dict

        @rtype: string

        """
        items = list(self.items())
        items.sort()
        return hash(repr(items))

    def keys(self):
        return self.__dict__.keys()

    def items(self):
        return self.__dict__.items()

    def update(self, dct):
        for k, v in dct.items():
            self[k] = v

    def save(self, filename):
        """Write a python file as a way of serializing a dictionary

        @type dct: dict
        @param dct: the input dictionary

        @type filename: string
        @param filename: filename to open (overwrite) to save dictionary contents

        @return None

        """
        f = open(filename, 'w')
        for key, val in self.items():
            repr_val = repr(val)
            assert val == eval(repr_val) #val can be re-loaded via eval()
            fmt = 0 #line format code 
            print >> f, repr((fmt, key, repr_val))
        f.close()

    def update_fromfile(self, filename):
        """Read local variables from a "key = val" file

        @type filename: string

        @param filename: an absolute or relative filename. This file will be opened for reading and read through.

        @rtype: None

        @note:
        This function reads a subset of valid python files.  All lines must be
        either comments, or assigment statements.  No imports, or class or
        function definitions are permitted.  Basically, this function reads
        back the sort of thing which could have been written by L{BasicDict.save}.

        """
        f = open(filename)
        for line in f:
            if not line: continue               #ignore empty lines
            if line.startswith('#'): continue   #ignore comments
            fmt, key, repr_val = eval(line)
            if fmt == 0:
                val = eval(repr_val)
            else:
                raise ValueError(line)
            self[key] = val
        f.close()

    @staticmethod
    def __checkkeyval__(key, val):
        conf = BasicDict()
        #must be string
        if type(key) != str:
            raise KeyError(key)
        #mustn't be part of BasicDict class interface
        if hasattr(conf, key):
            raise KeyError(key)
        #all alphanumeric
        for c in key:
            if c not in ('abcdefghijklmnopqrstuvwxyz'
                    'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
                    '_0123456789'):
                raise KeyError(key)
        #no symbols that look like reserved symbols
        if key[:2]=='__' and key[-2:]=='__' and len(key)>4:
            raise KeyError(key)

def load(*filenames):
    """Read the local variables from a python file

    @type filename: string
    @param filename: a file whose module variables will be returned as a
    dictionary

    @rtype: dict
    @return: the local variables of the imported filename

    @note:
    This implementation silently ignores all module symbols that don't meet
    the standards enforced by L{BasicDict.__checkkeys__}. This is meant to
    ignore module special variables.

    """
    o = BasicDict()
    for fname in filenames:
        o.update_fromfile(fname)
    return o

