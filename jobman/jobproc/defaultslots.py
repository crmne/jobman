"""Provides DefaultSlots"""
import StringIO

class DefaultSlots(object):
    """An object with named attributes.

    These named attributes can be over-ridden by the __init__, but no new attributes can be
    added by the __init__.  Attributes must all be declared at the class level.

    The set_slots() method can be used to assign to member attributes in a safe way that only
    over-rides attributes that are already present.

    It is possible to add new attributes manually at any time.
    """
    def __init__(self, **kwargs):
        self.set_slots(**kwargs)

    def set_slots(self, **kwargs):
        """Strictly reassign to member attributes.

        Attempt to create new member attributes will raise AttributeError (after some of the
        kwarg attributes may have been set).
        """
        for kw in kwargs:
            if hasattr(self, kw):
                setattr(self, kw, kwargs[kw])
                assert getattr(self, kw) == kwargs[kw]
            else:
                raise AttributeError(kw)

    def attr_iter(self):
        """Iterate over all the attributes of this class.
        """
        attrs_seen = set()
        for k in self.__dict__:
            if k in attrs_seen: continue
            yield k
            attrs_seen.add(k)
        for base in self.__class__.__mro__:
            for k in base.__dict__:
                if k in attrs_seen: continue
                yield k
                attrs_seen.add(k)

    def slots(self):
        """Iterate over all the 'normal' attributes of the class.

        Instance methods are skipped, as are any attributes that start with '__'.
        """
        instancemethod = type(DefaultSlots.slots)
        return [a 
                for a in self.attr_iter()
                if (not a.startswith('__') 
                    and (not isinstance(getattr(self, a), instancemethod)))]

    def __str__(self):
        return"%s{%s}"%(
                self.__class__.__name__,
                ', '.join(attr + ':'+str(getattr(self, attr)) for attr in self.slots()))

