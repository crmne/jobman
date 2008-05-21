
import threading, time, commands, os, sys

def reserve(jobtype, 
        max_retry=10,
        max_sleep=4.0):
    """Book a job.

    If no jobs remain, raise StopIteration
    
    @todo: is the jobtype mechanism necessary or sufficient?
    @todo: what about filtering based on the jobs (key,val) pairs? (pro: maybe more robust.  con: forces jobs to have (key,val) pairs)

    @jobtype: string identifier to filter potential jobs
    @param max_retry: try this many times to reserve a job before raising an exception
    @param max_sleep: sleep up to this many seconds between retry attempts

    @return job id
    """
    # obtain a serial connection to DB

    # select the first un-booked job

    # change that job's booked status to 'taken'

    # commit that change

    #if successful, return the ID

    #if unsuccessful, sleep for a random amount, and try again.

    raise NotImplementedError
    return 0

def get_desc(jid):
    """Return the description of job jid"""
    raise NotImplementedError()
def set_desc(jid, desc):
    """Set the description of job jid"""
    raise NotImplementedError()

def reset(jid, drop_results=False):
    """Reset job to be available for reservation.

    @param drop_results: delete results associated with job.

    If a job has results, but drop_results is false, an exception is raised.

    @return None
    
    """
    raise NotImplementedError
    return None
def finish(jid, dct):
    """Save and close a reserved job.

    @param jid: jobid reserved by this host
    @param dct: dictionary to save.  Keys must be strings, Values must be basic types: string, int, float.

    @return None

    @note: As a side effect, the state associated with job is deleted.

    """
    for k,v in dct.items():
        if type(k) is not str:
            raise TypeError('key', (k,v))
        if type(v) not in (str, int, float):
            raise TypeError('value', (k,v))

    raise NotImplementedError

    return None
def create(jobtype):
    """Create a new job.
    @return jid
    """
    raise NotImplementedError()

    return jid

def get_state(jid, blob):
    """Retrieve binary data associated with job."""
    raise NotImplementedError
def set_state(jid, blob):
    """Store binary data with job.
    This is useful for saving the state of a long program run.
    """
    raise NotImplementedError


def work(jobtype):
    try:
        jid = reserve(jobtype)
    except StopIteration:
        return

    def blah(*args):
        print 'blah: ', args
    dct = get_next()

if __name__ == '__main__':
    N = eval(sys.argv[1])

    for n in xrange(N):
        child_id = os.fork()
        if child_id == 0:
            work(n)
            break
