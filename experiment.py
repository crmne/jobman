
#These should be
INT = type(0)
FLT = type(0.0)
STR = type('')

COMPLETE = None    #jobs can return this by returning nothing as well
INCOMPLETE = True  #jobs can return this and be restarted


class Experiment(object):

    new_stdout = 'job_stdout'
    new_stderr = 'job_stderr'

    def remap_stdout(self):
        """
        Called before start and resume.

        Default behaviour is to replace sys.stdout with open(self.new_stdout, 'w+').

        """
        if self.new_stdout:
            sys.stdout = open(self.new_stdout, 'w+')

    def remap_stderr(self):
        """
        Called before start and resume.

        Default behaviour is to replace sys.stderr with open(self.new_stderr, 'w+').
        
        """
        if self.new_stderr:
            sys.stderr = open(self.new_stderr, 'w+')

    def tempdir(self):
        """
        Return the recommended filesystem location for temporary files.

        The idea is that this will be a fast, local disk partition, suitable
        for temporary storage.
        
        Files here will not generally be available at the time of resume().

        The return value of this function may be controlled by one or more
        environment variables.  
        
        Will return $DBDICT_EXPERIMENT_TEMPDIR if present.
        Failing that, will return $TMP/username-dbdict/hash(self)
        Failing that, will return /tmp/username-dbdict/hash(self)

        .. note::
            Maybe we should use Python stdlib's tempdir mechanism. 

        """

        print >> sys.stderr, "TODO: get tempdir correctly"
        return '/tmp/dbdict-experiment'


    def __init__(self, state):
        """Called once per lifetime of the class instance.  Can be used to
        create new jobs and save them to the database.   This function will not
        be called when a Job is retrieved from the database.

        Parent creates keys: dbdict_id, dbdict_module, dbdict_symbol, dbdict_status.

        """

    def start(self):
        """Called once per lifetime of the compute job.

        This is a good place to initialize internal variables.

        After this function returns, either stop() or run() will be called.
        
        dbdict_status -> RUNNING

        """

    def resume(self):
        """Called to resume computations on a previously stop()'ed job.  The
        os.getcwd() is just like it was after some previous stop() command.

        This is a good place to load internal variables from os.getcwd().

        dbdict_status -> RUNNING
        
        """
        return self.start()

    def run(self, channel):
        """Called after start() or resume().
        
        channel() may return different things at different times.  
            None   - run should continue.
            'stop' - the job should save state as soon as possible because
                     the process may soon be terminated

        When this function returns, dbdict_status -> DONE.
        """

