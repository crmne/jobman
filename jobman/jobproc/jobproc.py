"""A Job is supposed to be a pickle-able function execution.
"""
class JobProc(object):
    def __iter__(self):
        return self

    def next(self):
        """Override this method to perform useful work.
        Raise StopIteration() when that work is complete.
        """
        raise StopIteration()

    def run(self):
        for iter in self:
            pass
        return self
