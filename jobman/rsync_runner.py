"""
This runner provides a simple client-server pair for running jobs remotely.  

Communication and file syncronization between client and server is handled by the client (which
uses rsync).

The server is minimal: all it does is provide names of directories on the server that contain
jobs that need to be run.  It uses a semaphor to ensure that clients are issued unique jobs.

The client uses rsync to transfer the contents of the servers directory to a /tmp folder, calls
a function that it was provided on the cmdline, and then transfers the result back to the
server by rsync as well.

These rsync command is written to make use of some special directory structure:

    - *.no_sync_to_client will not be transfered from server to client.

    - *.no_sync will not be transfered either to client or to server.

    - symbolic links will be copied as links, unless they point to something outside of the
      rsync'ed folder in which case they will be copied by value.  This is the
      'copy-unsafe-links' semantics.
      QUESTION: What if links point into *.no_sync folder?

    - 'jobman_status' is created and managed by the client-side driver. It contains messages
      about starting and stopping of jobs... what time were they run, where, what machine.

    - 'jobman_stdout' is created and managed by the client-side driver.  sys.stdout appends to
      this file during the running of the callback.

    - 'jobman_stderr' is created and managed by the client-side driver.  sys.stderr appends to
      this file during the running of the callback.

    - directories beginning with PYTHONPATH will be prepended to sys.path before running the
      callback, and removed afterward.

"""
import os, random, logging, time, socket, sys, tempfile, datetime, traceback, shutil
from runner import runner_registry
from optparse import OptionParser

_logger = logging.getLogger('jobman.rsync_runner')
_logger.addHandler(logging.StreamHandler(sys.stderr))
_logger.setLevel(logging.DEBUG)

#################
# Salted hashing
#################
import hashlib

HASH_REPS = 2000 # some site said that this number could (should) be really high like 10K

def __saltedhash(string, salt):
    sha256 = hashlib.new('sha512')
    sha256.update(string)
    sha256.update(salt)
    for x in xrange(HASH_REPS): 
        sha256.update(sha256.digest())
        if x % 10: sha256.update(salt)
    return sha256

def saltedhash_bin(string, salt):
    """returns the hash in binary format"""
    return __saltedhash(string, salt).digest()

def saltedhash_hex(string, salt):
    """returns the hash in hex format"""
    return __saltedhash(string, salt).hexdigest()

###############
# Serving
###############

import SocketServer, time, logging, sys, os
import threading

class MyTCPHandler(SocketServer.BaseRequestHandler):
    """
    The RequestHandler class for our server.

    It is instantiated once per connection to the server, and must
    override the handle() method to implement communication to the
    client.
    """
    todo = '__todo__'
    done = '__done__'
    exproot = None # set externally
    lock = None  # set externally

    def handle(self):
        _logger.info('handling connection with %s' % str(self))
        # self.request is the TCP socket connected to the client

        # this is supposed to be some kind of basic security to prevent 
        # random internet probes from messing up my experiments.
        salt = repr(time.time())
        secret = self.exproot + os.getlogin()

        #self.data = self.request.recv(1024).strip()
        # just send back the salt
        self.request.send(salt)
        t0 = time.time()
        hashed_secret = saltedhash_bin(secret, salt)
        _logger.debug('hashing took %f'%(time.time() - t0))
        cli_hashed_secret = self.request.recv(512)
        if cli_hashed_secret == hashed_secret:
            _logger.info('client authenticated')
            self.request.send('ok')
            self.handle_authenticated()
        else:
            _logger.info('client failed authentication')
            self.request.send('err')

    def handle_authenticated(self):
        cmd = self.request.recv(1024)
        if cmd == 'job please':
            self.handle_job_please()
            pass
        else:
            raise NotImplementedError()

    def handle_job_please(self):
        self._lock.acquire()
        try:
            srclist = os.listdir(os.path.join(self.exproot, self.todo))
            if srclist:
                srclist.sort()
                choice = srclist[0] # the smallest value
                os.rename(
                        os.path.join(os.path.join(self.exproot, self.todo, choice)),
                        os.path.join(os.path.join(self.exproot, self.done, choice)))
            else:
                choice = ''
        except (OSError,IOError), e:
            _logger.error('Error booking ' + str(e))
            choice = ''
        finally:
            self._lock.release()

        _logger.debug('sending choice %s'% repr(choice))
        self.request.send(choice)

parser_serve = OptionParser(usage = '%prog serve [options] </path/to/experiment>',
        description=("Run a server for 'jobman rsync_any ...' commands.  "
            "The server will dequeue jobdirs from </path/to/experiment>/__todo__ and move them"
            " to </path/to/experiment>/__done__ as 'jobman rsync_any ...' processes ask for jobs."),
                          add_help_option=True)
parser_serve.add_option('--port', dest = 'port', type = 'int', default = 9999,
                      help = 'Serve on given port (default 9999)')
def runner_serve(options, path):
    """Run a server for remote jobman rsync_any commands (rsync_runner server).

    Example usage:

        jobman serve --port=9999 path/to/experiment

    The server will watch the path/to/experiment/__todo__ directory for names, 
    and move them to path/to/experiment/__done__ as clients connect [successfully] and
    ask for fresh jobs.  The client assumes responsibility for fetching and sync'ing files to
    this directory.
    """
    logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
    HOST, PORT = "localhost", options.port

    if not path.startswith('/'):
        # this is not strictly necessary, but done out of a desire for a simple interface.
        # The client connection string has user@host:port/path which suggests that the client
        # connection string path is fully-qualified.  The authentication is done by matching
        # experiment paths, so the server must have fully-qualified path as well.  We could
        # figure out fully-qualified path here automatically, but it is better that the user
        # can see plain as day on the screen that the paths of client and server match or don't
        # match.
        _logging.fatal("Error: Path must be fully-qualified")
        return -1

    _logger.info("Job server for %s listening on %s:%i" %
            (path, HOST, PORT))

    #install the _lock in the TCP handler for use by handle_job_please
    MyTCPHandler._lock = threading.Lock()
    MyTCPHandler.exproot = path

    # make sure the appropriate directories have been created
    try:
        os.listdir(os.path.join(path, MyTCPHandler.todo))
    except:
        os.makedirs(os.path.join(path, MyTCPHandler.todo))
    try:
        os.listdir(os.path.join(path, MyTCPHandler.done))
    except:
        os.makedirs(os.path.join(path, MyTCPHandler.done))

    # Create the server, binding to localhost on port 9999
    server = SocketServer.TCPServer((HOST, PORT), MyTCPHandler)

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    server.serve_forever()
runner_registry['serve'] = (parser_serve, runner_serve)


###############
# Client-side 
###############

class RSyncException(Exception):
    def __init__(self, cmd, rval):
        super(RSyncException, self).__init__('Rsync Failure', (cmd, rval))

def rsync(srcdir, dstdir, num_retries=3,
        options='-ac --copy-unsafe-links',
        exclusions=[]): 
    excludes = ' '.join('--exclude="%s"' % e for e in exclusions)
    raw_cmd = 'rsync %(options)s %(excludes)s "%(srcdir)s/" "%(dstdir)s/"'
    rsync_cmd = raw_cmd % locals()

    keep_trying = True
    rsync_rval = 1 # some non-null value

    # allow n-number of retries, with random hold-off between retries
    attempt = 0
    while rsync_rval!=0 and keep_trying:
        _logger.debug('executing rsync command: %s'%rsync_cmd)
        rsync_rval = os.system(rsync_cmd)

        if rsync_rval != 0:
            _logger.info('rsync error %i' % rsync_rval)
            attempt += 1
            keep_trying = attempt < num_retries
            # wait anywhere from 30s to [2,4,6] mins before retrying
            if keep_trying: 
                r = random.randint(30,attempt*120)
                _logger.warning( 'RSync Error at %s attempt %i/%i: sleeping %is' %(
                            rsync_cmd, attempt,num_retries,r))
                time.sleep(r)

    if rsync_rval != 0:
        raise RSyncException(rsync_cmd, rsync_rval)

def server_getjob(user, host, port, expdir):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    salt = s.recv(512)
    secret =  expdir + user
    _logger.debug('secret %s'% secret)
    _logger.debug('salt %s'% salt)
    s.send(saltedhash_bin(secret, salt))
    sts = s.recv(1024)
    if sts == 'ok':
        s.send('job please')
        jobname = s.recv(1024)
        s.close()
    else:
        s.close()
        raise Exception("failed to authenticate")
    return jobname

def parse_server_str(fulladdr):
    # user@host:port/full/path/to/expdir
    user = fulladdr[:fulladdr.index("@")]
    host = fulladdr[fulladdr.index("@")+1:fulladdr.index(':')]
    port = int(fulladdr[fulladdr.index(":")+1:fulladdr.index('/')])
    expdir = fulladdr[fulladdr.index("/"):]
    return user, host, port, expdir

def run_callback_in_rsynced_tempdir(remote_rsync_loc, callback, 
        callbackname,
        redirect_stdout='jobman_stdout',
        redirect_stderr='jobman_stderr',
        status_filename='jobman_status',
        logger=None):

    # get a local tmpdir
    tmpdir = tempfile.mkdtemp()

    # rsync from remote directory to tmpdir
    rsync(remote_rsync_loc, tmpdir, exclusions=['*.no_sync_to_client', '*.no_sync'])

    # chdir to tmpdir
    os.chdir(tmpdir)

    # redirect stdout, stderr
    stdout = sys.stdout
    stderr = sys.stderr

    try:
        stderr_handler = None
        if redirect_stderr:
            sys.stderr = open(redirect_stderr, 'a+')
        if logger is None:
            logger = logging.getLogger('jobman.rsync_callback_runner')
        stderr_handler = logging.StreamHandler(sys.stderr)
        logger.addHandler(stderr_handler)
        if redirect_stdout:
            sys.stdout = open(redirect_stdout, 'a+')

        # Add PYTHONPATH dirs in cwd to sys.path
        new_pythonpaths = [os.path.join(tmpdir, f) 
                for f in os.listdir(tmpdir) 
                if os.path.isdir(os.path.join(tmpdir, f))]
        # it is important to impose an order, since listdir() doesn't guarantee anything
        new_pythonpaths.sort()
        new_pythonpaths.reverse() #largest elements (by string cmp) are placed first
        logger.info('Prepending to sys.path: %s'% str(new_pythonpaths))
        sys.path[0:0] = new_pythonpaths


        # rsync back to the remote directory
        if status_filename:
            localhost = socket.gethostname()
            statusfile = open(status_filename, 'a+')
            now = str(datetime.datetime.now())
            print >> statusfile, "%(now)s Running %(callbackname)s in %(localhost)s:%(tmpdir)s" % locals()
        rsync(tmpdir, remote_rsync_loc, exclusions=['*.no_sync_to_server', '*.no_sync'])

        try:
            callback()
        except Exception, e:
            traceback.print_exc() # goes to sys.stderr, aka redirect_stderr
            if status_filename:
                print >> statusfile, "%(now)s Failure in %(callbackname)s, see stderr for details." % locals()
                statusfile.flush()

        # Del the sys.path entries from sys.path after running callback
        if sys.path[0:len(new_pythonpaths)] == new_pythonpaths:
            logger.info('Removing from sys.path: %s'% str(new_pythonpaths))
            del sys.path[0:len(new_pythonpaths)]
        else:
            logger.warning('sys.path has been modified by callback, not removing %s' %
                    str(new_pythonpaths))

        if status_filename:
            now = str(datetime.datetime.now())
            print >> statusfile, "%(now)s Done %(callbackname)s in %(localhost)s:%(tmpdir)s" % locals()
            statusfile.flush()

        sys.stdout.flush()
        sys.stderr.flush()
        # rsync back to the remote directory
        # if this fails and raises an exception, the rmtree below is skipped so the files
        # remain on disk.
        rsync(tmpdir, remote_rsync_loc, exclusions=['*.no_sync_to_server', '*.no_sync'])

        # delete the tempdir
        shutil.rmtree(tmpdir, ignore_errors=True)
        #lambda fn, path, excinfo : sys.stderr.writeline(
        #    'Error in rmtree: %s:%s:%s' % (fn, path, excinfo)))

    finally:
        # return stdout, stderr
        sys.stdout = stdout
        sys.stderr = stderr
        if stderr_handler:
            logger.removeHandler(stderr_handler)
    # return None

parser_rsyncany = OptionParser(
        usage='%prog rsync_any [options] <user@server:port/fullpath/to/experiment> <module.function()>',
        description=("Run <module.function()> in a local tmpdir that is rsync'd with any one of the jobs"
            " waiting on the server.  This will dequeue the job on the server, so no "
            "other process will do the same job.\n\n"
            "The same function should be used for all the jobs"
            " on the server at any given time because you cannot always control which job will be dequeued."),
        add_help_option=True)
parser_rsyncany.add_option('--stderr', dest='stderr', type='str', metavar='FILE', default='jobman_stderr',
                      help = 'direct sys.stderr to FILE (Default "jobman_stderr")')
parser_rsyncany.add_option('--stdout', dest='stdout', type='str', metavar='FILE', default='jobman_stdout',
                      help = 'direct sys.stdout to FILE (Default "jobman_stdout")')
parser_rsyncany.add_option('--status', dest='status', type='str', metavar='FILE', default='jobman_status',
                      help = 'direct status messages to FILE (Default "jobman_status")')
def import_cmd(cmd):
    """Return the full module name of a fully-quallified function call
    """
    #print 'cmd', cmd
    lp = cmd.index('(')
    ftoks = cmd[:lp].split('.')
    imp = '.'.join(ftoks[:-1])
    return imp, cmd

# list of dictionaries, usually of length 
#   - 0, when runner_rsyncany is not running) or,
#   - 1, when runner_rsyncany is running a job.
# If runner_rsyncany were called recursively, this list would have lenght > 1.
# But I can't think of why or how that should ever happen.
_remote_info = []
def remote_info():
    return _remote_info[-1]
def remote_ssh():
    return 'ssh://%(user)s@%(host)s' % remote_info()
def remote_rsync_loc():
    return '%(user)s@%(host)s:/%(jobdir)s' % remote_info()
def _rsyncany_helper(imp, cmd):
    if imp:
        logging.getLogger('pyrun').debug('Importing: %s' % imp)
        exec('import '+imp)
    logging.getLogger('pyrun').debug('executing: %s' % cmd)
    exec(cmd)
def runner_rsyncany(options, addr, fullfn):
    """Run a function (on any job) in an rsynced tempdir (rsync_runner client).

    Example usage:

        jobman rsync_any bergstra@gershwin:9999/fullpath/to/experiment 'mymodule.function()'

    """

    _logger.setLevel(logging.DEBUG)
    #parse the server address
    user, host, port, expdir = parse_server_str(addr)
    _logger.debug('server addr: %s %s %s %s' % (user, host, port, expdir))

    # book a job from the server (get a remote directory)
    # by moving any job from the todo subdir to the done subdir
    jobname = server_getjob(user, host, port, expdir)
    if jobname == '':
        print "No more jobs"
        return
    _logger.info('handling jobname: %s' % jobname)
    jobdir = os.path.join(expdir, jobname)
    _remote_info.append(locals())
    try:
        # run that job
        run_callback_in_rsynced_tempdir(
                remote_rsync_loc(),
                lambda : _rsyncany_helper(*import_cmd(fullfn)),
                callbackname=fullfn,
                redirect_stdout=options.stdout,
                redirect_stderr=options.stderr,
                status_filename=options.status,
                )
    finally:
        _remote_info.pop()

runner_registry['rsync_any'] = (parser_rsyncany, runner_rsyncany)

