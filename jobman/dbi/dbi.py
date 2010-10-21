#! /usr/bin/env python
import datetime
import os
import re
import shutil
import string
import subprocess
from subprocess import Popen,PIPE,STDOUT
import sys
from textwrap import dedent
from threading import Lock,Thread
import time
from time import sleep
import traceback

from utils import get_condor_platform, get_config_value, get_plearndir, get_new_sid, set_config_value, truncate

try:
    from random import shuffle
except ImportError:
    import whrandom
    def shuffle(list):
        l = len(list)
        for i in range(0,l-1):
            j = whrandom.randint(i+1,l-1)
            list[i], list[j] = list[j], list[i]

STATUS_FINISHED = 0
STATUS_RUNNING = 1
STATUS_WAITING = 2
STATUS_INIT = 3
MAX_FILENAME_SIZE=255

class DBIError(Exception):
    """Base class for exceptions in this module."""
    pass

#original version from: http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/196618
class LockedIterator:
    def __init__( self, iterator ):
        self._lock     = Lock()
        self._iterator = iterator

    def __iter__( self ):
        return self

    def get(self):
        try:
            self._lock.acquire()
            return self._iterator.next()
        finally:
            self._lock.release()

    def nb_left( self ):
        try:
            self._lock.acquire()
            return self._iterator.__length_hint__()
        finally:
            self._lock.release()

    def next( self ):
        try:
            self._lock.acquire()
            return self._iterator.next()
        finally:
            self._lock.release()

class LockedListIter:
    def __init__( self, list ):
        self._lock     = Lock()
        self._list     = list
        self._last     = -1

    def __iter__( self ):
        return self

    def next(self):
        try:
            self._lock.acquire()
            self._last+=1
            if len(self._list)>self._last:
                return
            else:
                return self._list[self._last]
        finally:
            self._lock.release()

    def nb_left( self ):
        try:
            self._lock.acquire()
            return len(self._list) - self._last - 1
        finally:
            self._lock.release()

    def append( self, a ):
        try:
            self._lock.acquire()
            list.append(a)
        finally:
            self._lock.release()


#original version from: http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/196618
class MultiThread:
    def __init__( self, function, argsVector, maxThreads=5, print_when_finished=None, sleep_time = 0):
        self._function     = function
        self._argsIterator = LockedIterator( iter( argsVector ) )
        self._threadPool   = []
        self.maxThreads_file = None
        self.print_when_finish = print_when_finished
        self.running = 0
        self.init_len_list = len(argsVector)
        self.sleep_time = sleep_time

        if maxThreads==-1:
            nb_thread=len(argsVector)
        elif isinstance(maxThreads,str):
            self._lock_threadPool = Lock()
            self.maxThreads_file = maxThreads
            nb_thread = 0#Thread will be created when self.start() is called.
        elif maxThreads<=0:
            raise DBIError("[DBI] ERROR: you set %d concurrent jobs. Must be higher then 0!!"%(maxThreads))
        else:
            nb_thread=maxThreads
        if nb_thread>len(argsVector):
            nb_thread=len(argsVector)

        self.update_nb_thread(nb_thread, False)

    def parse_maxThreads_file( self ):
        """ return the number of Thread to use give in a file        
        """
        f = open( self.maxThreads_file )
        nb_thread = f.readlines()
        f.close()
        return int( nb_thread[0] )

    def update_nb_thread(self, nb_thread, by_running_threads):
        """ Update the thread pool to the good number of thread
            Return False when the running Thread should stop.

            :type nb_thread: int
            :param nb_thread: The number of threads wanted
            :type by_running_threads: bool
            :param by_running_threads: Must be True when called from a 
                                       Thread in the pool 
        """
        try:
            self._lock_threadPool.acquire()
            left = self._argsIterator.nb_left()
            if nb_thread==-1:
                if left<=0:
                    return False
                else:
                    nb_thread = len( self._threadPool ) + left
                    if by_running_threads:
                    #-1 as we reuse the current thread
                        nb_thread-=1

            elif nb_thread > (len( self._threadPool )+ left):
                #we don't generate more new thread then the nuber of jobs left
                nb_thread = len( self._threadPool ) + left
                if by_running_threads:
                    #-1 as we reuse the current thread
                    nb_thread-=1

            
            if nb_thread != len( self._threadPool ):
                if nb_thread < len( self._threadPool ):
                    if by_running_threads:
                        #I don't remove the thread from the pool as this
                        #seam to end a thread too early.
                        #If we raise the number of thread after
                        #we end up with too much thread in the pool
                        #But next time we start the pool, we will 
                        #resize it correctly.
                        #self._threadPool.remove(currentThread())
                        pass
                    else:
                        self._threadPool = self._threadPool[:nb_thread]
                    return False
                else:
                    for i in range( nb_thread - len( self._threadPool ) ):
                        self._threadPool.append( Thread( target=self._tailRecurse ) )
                        if by_running_threads:
                            time.sleep( self.sleep_time )
                            self.running+=1
                            self._threadPool[-1].start()
            return True
        finally:
            self._lock_threadPool.release()


    def _tailRecurse( self ):
        for args in self._argsIterator:
            self._function( args )
            if self.maxThreads_file:
                ret = self.update_nb_thread(self.parse_maxThreads_file(), True)
                if not ret:
                    break
        self.running-=1
        if self.print_when_finish:
            if callable(self.print_when_finish):
                print self.print_when_finish(),"left running: %d/%d"%(self.running,self.init_len_list)
            else:
                print self.print_when_finish,"left running: %d/%d"%(self.running,self.init_len_list)

    def start( self  ):
        if self.maxThreads_file:
        #update the number of thread
            self.update_nb_thread(self.parse_maxThreads_file(), False)
                    
        for thread in self._threadPool:
            # necessary to give other threads a chance to run
            time.sleep( self.sleep_time )
            self.running+=1
            thread.start()

    def join( self, timeout=None ):
        for thread in self._threadPool:
            thread.join( timeout )

class DBIBase:

    def __init__(self, commands, **args ):
        #generate a new unique id
        self.unique_id = get_new_sid('')

        # option is not used yet
        self.has_short_duration = 0

        # if all machines are full, run the jobs one by one on the localhost
        self_use_localhost_if_full = 1

        # the( human readable) time format used in log file
        self.time_format = "%Y-%m-%d/%H:%M:%S"

        # Commands to be executed once before the entire batch on the submit node
        self.pre_batch = []
        # Commands to be executed before every task in tasks
        self.pre_tasks = []
        # The main tasks to be dispatched
        self.tasks = []
        # Commands to be executed after each task in tasks
        self.post_tasks = []
        # Commands to be executed once after the entire batch on the submit node
        self.post_batch = []

        # the default directory where to keep all the log files
        self.log_dir = os.path.join( 'LOGS', self.unique_id )
        self.log_file = os.path.join( self.log_dir, 'log' )

        # the default directory where file generated by dbi will be stored
        # It should not take the "" or " " value. Use "." instead.
        self.tmp_dir = 'TMP_DBI'
        #
        if not hasattr(self, 'file_redirect_stdout'):
            self.file_redirect_stdout = True
        if not hasattr(self, 'file_redirect_stderr'):
            self.file_redirect_stderr = True
        if not hasattr(self, 'redirect_stderr_to_stdout'):
            self.redirect_stderr_to_stdout = False

        # Initialize the namespace
        self.test = False
        self.dolog = False
        self.temp_files = []
        self.arch = 0 # TODO, we should put the local arch: 32,64 or 3264 bits
        self.base_tasks_log_file = []
        self.stdouts = ''
        self.stderrs = ''
        self.raw = ''
        self.cpu = 1
        self.mem = "0"

        for key in args.keys():
            self.__dict__[key] = args[key]

        # check if log directory exists, if not create it
        if (not os.path.exists(self.log_dir)):
#            if self.dolog or self.file_redirect_stdout or self.file_redirect_stderr:
            os.mkdir(self.log_dir)
        if self.mem[-1] in ['G', 'g']:
            self.mem = int(float(self.mem[:-1])*1024)
        elif self.mem[-1] in ['M', 'm']:
            self.mem = int(self.mem[:-1])
        elif self.mem[-1] in ['K', 'k']:
            self.mem = int(self.mem[:-1])/1024
        else: self.mem = int(self.mem)
            
        self.cpu = int(self.cpu)
        # If some arguments aren't lists, put them in a list
        if not isinstance(commands, list):
            commands = [commands]
        if not isinstance(self.pre_batch, list):
            self.pre_batch = [self.pre_batch]
        if not isinstance(self.pre_tasks, list):
            self.pre_tasks = [self.pre_tasks]
        if not isinstance(self.post_tasks, list):
            self.post_tasks = [self.post_tasks]
        if not isinstance(self.post_batch, list):
            self.post_batch = [self.post_batch]

    def n_avail_machines(self): raise NotImplementedError, "DBIBase.n_avail_machines()"

    def add_commands(self,commands): raise NotImplementedError, "DBIBase.add_commands()"

    def get_file_redirection(self, task_id):
        """ Calculate the file to use for stdout/stderr
        """
        n=task_id-1
        base=self.tasks[n].log_file
        if self.base_tasks_log_file and self.base_tasks_log_file[n]:
            base = self.base_tasks_log_file[n]
            base=os.path.join(self.log_dir,base)
            self.check_path(base)
        elif self.stdouts and self.stderrs:
            assert len(self.stdouts)==len(self.stderrs)==len(self.tasks)
            return (self.stdouts[n], self.stderrs[n])

        return (base + '.out',base + '.err')
            

    def get_redirection(self,stdout_file,stderr_file):
        """Compute the needed redirection based of the objects attribute.
        Return a tuple (stdout,stderr) that can be used with popen.
        """
        output = PIPE
        error = PIPE
        if int(self.file_redirect_stdout):
            self.check_path(stdout_file)
            output = open(stdout_file, 'w')
        if self.redirect_stderr_to_stdout:
            error = STDOUT
        elif int(self.file_redirect_stderr):
            self.check_path(stderr_file)
            error = open(stderr_file, 'w')
        return (output,error)

    def exec_pre_batch(self):
        # Execute pre-batch
        if len(self.pre_batch)>0:
            pre_batch_command = ';'.join( self.pre_batch )
            if not self.test:
                (output,error)=self.get_redirection(self.log_file + '.out',self.log_file + '.err')
                self.pre = Popen(pre_batch_command, shell=True, stdout=output, stderr=error)
            else:
                print "[DBI] pre_batch_command:",pre_batch_command

    def exec_post_batch(self):
        # Execute post-batch
        if len(self.post_batch)>0:
            post_batch_command = ';'.join( self.post_batch )
            if not self.test:
                (output,error)=self.get_redirection(self.log_file + '.out',self.log_file + '.err')
                self.post = Popen(post_batch_command, shell=True, stdout=output, stderr=error)
            else:
                print "[DBI] post_batch_command:",post_batch_command

    def clean(self):
        print "[DBI] WARNING the clean function was not overrided by the sub class!"

    def run(self):
        pass

    def wait(self):
        print "[DBI] WARNING the wait function was not overrided by the sub class!"

    def print_jobs_status(self):
        finished=0
        running=0
        waiting=0
        init=0
        unfinished=[]
        for t in self.tasks:
            if t.status==STATUS_INIT:
                init+=1
                unfinished.append(t.id)
            elif t.status==STATUS_RUNNING:
                running+=1
                unfinished.append(t.id)
            elif t.status==STATUS_FINISHED:
                finished+=1
            elif t.status==STATUS_WAITING:
                waiting+=1
                unfinished.append(t.id)
            else:
                print "[DBI] jobs %i have an unknow status: %d",t.id
        print "[DBI] %d jobs. finished: %d, running: %d, waiting: %d, init: %d"%(len(self.tasks),finished, running, waiting, init)
        print "[DBI] jobs unfinished (starting at 1): ",unfinished

    def check_path(self, p):
        """
        A function that check we use a path to file that is valid.
        We currently check that each directory and the file in the path
        are not too long.
        """
        l = [p]
        while True:
            sp=os.path.split(l[0])
            if sp[0]=="": break
            if sp[1]=="": l[0]=sp[0];break
            l.append(sp[1])
            l[0]=sp[0]
        for pp in l:
            if len(pp)> MAX_FILENAME_SIZE:
                raise DBIError("ERROR: a path containt a diretory or a filename that "+
                               " is too long, so the jobs will fail. Maybe"+
                               " use the --tasks_filename option to change those name.\n"+
                               "The full bad path: "+p+"\n"+"The bad part: "+pp)
                


class Task:

    def __init__(self, command, tmp_dir, log_dir, time_format, pre_tasks=[], post_tasks=[], dolog = True, id=-1, gen_unique_id = True, args = {}):
        self.add_unique_id = 0
        self.id=id
        # The "python utils.py..." command is not exactly the same for every
        # task in a batch, so it cannot be considered a "pre-command", and
        # has to be actually part of the command.  Since the user-provided
        # pre-command has to be executed afterwards, it also has to be part of
        # the command itself. Therefore, no need for pre- and post-commands in
        # the Task class

        utils_file = os.path.join(tmp_dir, 'utils.py')
        utils_file = os.path.abspath(utils_file)

        for key in args.keys():
            self.__dict__[key] = args[key]
        self.dolog = dolog

        formatted_command = re.sub( '[^a-zA-Z0-9]', '_', command );
        if gen_unique_id:
            self.unique_id = get_new_sid('')#compation intense
            self.log_file = truncate( os.path.join(log_dir, self.unique_id +'_'+ formatted_command), 200) + ".log"
        else:
            self.unique_id = formatted_command[:200]+'_'+str(datetime.datetime.now()).replace(' ','_').replace(':','-')
            self.log_file = os.path.join(log_dir, self.unique_id) + ".log"

        if self.add_unique_id:
                command = command + ' unique_id=' + self.unique_id
        #self.before_commands = []
        #self.user_defined_before_commands = []
        #self.user_defined_after_commands = []
        #self.after_commands = []

        self.commands = []
        if len(pre_tasks) > 0:
            self.commands.extend( pre_tasks )

        if self.dolog == True:
            self.commands.append(utils_file + ' set_config_value '+
                string.join([self.log_file,'STATUS',str(STATUS_RUNNING)],' '))
            # set the current date in the field LAUNCH_TIME
            self.commands.append(utils_file +  ' set_current_date '+
                string.join([self.log_file,'LAUNCH_TIME',time_format],' '))


        self.commands.append( command )
        self.commands.extend( post_tasks )
        if self.dolog == True:
            self.commands.append(utils_file + ' set_config_value '+
                string.join([self.log_file,'STATUS',str(STATUS_FINISHED)],' '))
            # set the current date in the field FINISHED_TIME
            self.commands.append(utils_file + ' set_current_date ' +
                string.join([self.log_file,'FINISHED_TIME',time_format],' '))

        #print "self.commands =", self.commands
        self.status=STATUS_INIT
    def get_status(self):
        #TODO: catch exception if value not available
        status = get_config_value(self.log_file,'STATUS')
        return int(status)

    def get_stdout(self):
        try:
            if isinstance(self.p.stdout, file):
                return self.p.stdout
            else:
                return open(self.log_file + '.out','r')
        except:
            pass
        return None

    def get_stderr(self):
        try:
            if isinstance(self.p.stderr, file):
                return self.p.stderr
            else:
                return open(self.log_file + '.err','r')
        except:
            pass
        return None

    def set_scheduled_time(self):
        if self.dolog:
            set_config_value(self.log_file, 'STATUS',str(STATUS_WAITING))
            set_config_value(self.log_file, 'SCHEDULED_TIME',
                             time.strftime(self.time_format, time.localtime(time.time())))

    def get_waiting_time(self):
        # get the string representation
        str_sched = get_config_value(self.log_file,'SCHEDULED_TIME')
        # transform in seconds from the start of epoch
        sched_time = time.mktime(time.strptime(str_sched,self.time_format))

        # get the string representation
        str_launch = get_config_value(self.log_file,'LAUNCH_TIME')
        # transform in seconds from the start of epoch
        launch_time = time.mktime(time.strptime(str_launch,self.time_format))

        return launch_time - sched_time

    def get_running_time(self):
        #TODO: handle if job did not finish
        # get the string representation
        str_launch = get_config_value(self.log_file,'LAUNCH_TIME')
        # transform in seconds from the start of epoch
        launch_time = time.mktime(time.strptime(str_launch,self.time_format))

        # get the string representation
        str_finished = get_config_value(self.log_file,'FINISHED_TIME')
        # transform in seconds from the start of epoch
        finished_time = time.mktime(time.strptime(str_finished,self.time_format))

        return finished_time - launch_time

class DBICluster(DBIBase):

    def __init__(self, commands, **args ):
        self.duree=None
        self.arch=None
        self.cwait=True
        self.force=False
        self.interruptible=False
        self.threads=[]
        self.started=0
        self.nb_proc=32
        self.mt=None
        self.args=args
        self.os=None
        DBIBase.__init__(self, commands, **args)

        if self.os:
            self.os = self.os.lower()

        self.pre_tasks=["echo '[DBI] executing on host' $HOSTNAME"]+self.pre_tasks
        self.post_tasks=["echo '[DBI] exit status' $?"]+self.post_tasks
        self.add_commands(commands)
        self.nb_proc=int(self.nb_proc)
        self.backend_failed=0
        self.jobs_failed=0

        if not os.path.exists(self.tmp_dir):
            os.mkdir(self.tmp_dir)

    def add_commands(self,commands):
        if not isinstance(commands, list):
            commands=[commands]

        # create the information about the tasks
        id=len(self.tasks)+1
        for command in commands:
            self.tasks.append(Task(command, self.tmp_dir, self.log_dir,
                                   self.time_format,self.pre_tasks,
                                   self.post_tasks,self.dolog,id,False,
                                   self.args))
            id+=1


    def run_one_job(self, task):
        DBIBase.run(self)
        task.status=STATUS_RUNNING

        remote_command=string.join(task.commands,';')
        filename=os.path.join(self.tmp_dir,task.unique_id)
        filename=os.path.abspath(filename)
        f=open(filename,'w')
        f.write(remote_command+'\n')
        f.close()
        os.chmod(filename, 0750)
        self.temp_files.append(filename)

        command = "cluster"
        if self.arch == "32":
            command += " --typecpu 32bits"
        elif self.arch == "64":
            command += " --typecpu 64bits"
        elif self.arch == "3264":
            command += " --typecpu all"
        elif os.uname()[4]=="x86_64":
            #by default the cluster send to 32 bits computers
            #we want that by default we use the same arch as the submit computer!
            command += " --typecpu 64bits"
        elif os.uname()[4] in ["i686", "i386"]:
            command += " --typecpu 32bits"
        if self.duree:
            command += " --duree "+self.duree
        if self.cwait:
            command += " --wait"
        if self.mem > 0:
            command += " --memoire "+self.mem
        if self.force:
            command += " --force"
        if self.interruptible:
            command += " --interruptible"
        if self.cpu>0:
            command += " --cpu " + str(self.cpu)
        if self.os:
            command += " --os "+self.os
        command += " --execute '"+ filename + "'"

        self.started+=1
        started=self.started# not thread safe!!!
        print "[DBI, %d/%d, %s] %s"%(started,len(self.tasks),time.ctime(),command)
        if self.test:
            task.status=STATUS_FINISHED
            return

        task.launch_time = time.time()
        task.set_scheduled_time()

        (output_file, error_file)=self.get_file_redirection(task.id)
        (output, error)=self.get_redirection(output_file, error_file)
        task.p = Popen(command, shell=True,stdout=output,stderr=error)
        task.p_wait_ret=task.p.wait()
        task.dbi_return_status=None
        if output!=PIPE:#TODO what do to if = PIPE?
            fd=open(output_file,'r')
            last=""
            for l in fd.readlines():
                last=l
            if last.startswith("[DBI] exit status "):
                task.dbi_return_status=int(last.split()[-1])
#        print "[DBI,%d/%d,%s] Job ended, popen returncode:%d, popen.wait.return:%d, dbi echo return code:%s"%(started,len(self.tasks),time.ctime(),task.p.returncode,task.p_wait_ret,task.dbi_return_status)
        if task.dbi_return_status==None:
            print "[DBI, %d/%d, %s] Trouble with launching/executing '%s'." % (started,len(self.tasks),time.ctime(),command)
            print "    Its execution did not finished. Probable cause is the back-end itself."
            print "    You may want to run the task again."
            print "    popen returncode: %d"     % task.p.returncode
            print "    popen.wait.return: %d"    % task.p_wait_ret
            print "    dbi echo return code: %s" % task.dbi_return_status
            self.backend_failed+=1
        elif task.dbi_return_status!=0:
            self.jobs_failed+=1
        task.status=STATUS_FINISHED

    def run(self):
        print "[DBI] The Log file are under %s"%self.log_dir
        if self.test:
            print "[DBI] Test mode, we only print the command to be executed, we don't execute them"
        # Execute pre-batch
        self.exec_pre_batch()

        # Execute all Tasks (including pre_tasks and post_tasks if any)
        self.mt=MultiThread(self.run_one_job,self.tasks,
                            self.nb_proc,lambda :"[DBI,%s]"%time.ctime(),
                            sleep_time=2)
        self.mt.start()

        # Execute post-batchs
        self.exec_post_batch()

        print "[DBI] The Log file are under %s"%self.log_dir

    def clean(self):
        #TODO: delete all log files for the current batch
        for f in self.temp_files:
            os.remove(f)

    def wait(self):
        if self.mt:
            try:
                self.mt.join()
            except KeyboardInterrupt, e:
                print "[DBI] Catched KeyboardInterrupt"
                self.print_jobs_status()
                raise

        else:
            print "[DBI] WARNING jobs not started!"
        self.print_jobs_status()
        print "[DBI] %d jobs where the back-end failed." % (self.backend_failed)
        print "[DBI] %d jobs returned a failure status." % (self.jobs_failed)

class DBIBqtools(DBIBase):

    def __init__( self, commands, **args ):
        self.nb_proc = -1
        self.clean_up = True
        self.micro = 0
        self.nano = 0
        self.queue = "qwork@ms"
        self.long = False
        self.duree = "120:00:00"
        self.submit_options = ""
        self.jobs_name = ""
        self.m32G = False
        self.set_special_env = True
        self.env = ""
        DBIBase.__init__(self, commands, **args)
        
        self.nb_proc = int(self.nb_proc)
        self.micro = int(self.micro)
        self.nano = int(self.nano)

        if self.set_special_env and self.cpu>0:
            self.env+=' OMP_NUM_THREADS=%d GOTO_NUM_THREADS=%d MKL_NUM_THREADS=%d'%(self.cpu,self.cpu,self.cpu)
        if self.env:
            self.env='export '+self.env
### We can't accept the symbols "," as this cause trouble with bqtools
        if self.log_dir.find(',')!=-1 or self.log_file.find(',')!=-1:
            raise DBIError("[DBI] ERROR: The log file(%s) and the log dir(%s) should not have the symbol ','"%(self.log_file,self.log_dir))

        # create directory in which all the temp files will be created
        self.tmp_dir = os.path.join(self.tmp_dir,os.path.split(self.log_dir)[1])
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        print "[DBI] All bqtools file will be in ",self.tmp_dir
        os.chdir(self.tmp_dir)

        if self.long:
            # Get max job duration from environment variable if it is set.
            max = os.getenv("BQ_MAX_JOB_DURATION")
            if max:
                self.duree = max
            else:
                self.duree = "1200:00:00" #50 days

        # create the information about the tasks
        args['tmp_dir'] = self.tmp_dir
        self.args=args
        self.add_commands(commands)

    def add_commands(self,commands):
        if not isinstance(commands, list):
            commands=[commands]

        # create the information about the tasks
        for command in commands:
            id=len(self.tasks)+1
            self.tasks.append(Task(command, self.tmp_dir, self.log_dir,
                                   self.time_format,self.pre_tasks,
                                   self.post_tasks,self.dolog,id,False,
                                   self.args))
            id+=1
    def run(self):
        pre_batch_command = ';'.join( self.pre_batch );
        post_batch_command = ';'.join( self.post_batch );

        # create one (sh) script that will launch the appropriate ~~command~~
        # in the right environment


        launcher = open( 'launcher', 'w' )
        bq_cluster_home = os.getenv( 'BQ_CLUSTER_HOME', '$HOME' )
        bq_shell_cmd = os.getenv( 'BQ_SHELL_CMD', '/bin/sh -c' )
        launcher.write( dedent('''\
                #!/bin/sh

                HOME=%s
                export HOME

                %s
                cd ../../../../
                (%s '~~task~~')'''
                % (bq_cluster_home, self.env, bq_shell_cmd)
                ) )

        if int(self.file_redirect_stdout):
            launcher.write( ' >> ~~logfile~~.out' )
        if int(self.file_redirect_stderr):
            launcher.write( ' 2>> ~~logfile~~.err' )
        launcher.close()

        # create a file containing the list of commands, one per line
        # and another one containing the log_file name associated
        tasks_file = open( 'tasks', 'w' )
        logfiles_file = open( 'logfiles', 'w' )
        assert len(self.stdouts)==len(self.stderrs)==0
        for task in self.tasks:
            #-4 as we will append .err or .out.
            base=self.get_file_redirection(task.id)[0][:-4]
            self.check_path(base)
            tasks_file.write( ';'.join(task.commands) + '\n' )
            logfiles_file.write( base + '\n' )

        tasks_file.close()
        logfiles_file.close()

        tmp_options = self.submit_options
        if self.queue:
            tmp_options+=" -q "+self.queue
        l=""
        if self.cpu >0:
            l+="ncpus="+str(self.cpu)
        if self.mem >0:
            if l: l+=","
            l="mem=%dM"%(self.mem)
        if self.duree:
            if l: l+=","
            l+="walltime="+self.duree
        if self.m32G:
            if l: l+=","
            l+="nodes=1:m32G"
        if l:
            tmp_options+=" -l "+l
        batchName = self.jobs_name
        if not batchName:
            batchName = "dbi_"+self.unique_id[1:12]

        # Create the bqsubmit.dat, with
        bqsubmit_dat = open( 'bqsubmit.dat', 'w' )
        bqsubmit_dat.write( dedent('''\
                batchName = %s
                command = sh launcher
                templateFiles = launcher
                submitOptions = %s
                param1 = (task, logfile) = load tasks, logfiles
                linkFiles = launcher
                preBatch = rm -f _*.BQ
                '''%(batchName,tmp_options)))
        if self.micro>0:
            bqsubmit_dat.write('''microJobs = %d\n'''%(self.micro))
        if self.nano>0:
            bqsubmit_dat.write('''nanoJobs = %d\n'''%(self.nano))
        p=self.nb_proc
        if p==-1:
            p=len(self.tasks)
        if p>0:
            bqsubmit_dat.write('''concurrentJobs = %d\n'''%(p))
        if self.raw:
            bqsubmit_dat.write(self.raw+"\n")
            
        print self.unique_id
        if self.clean_up:
            bqsubmit_dat.write('postBatch = rm -rf dbi_batch*.BQ ; rm -f logfiles tasks launcher bqsubmit.dat ;\n')
        bqsubmit_dat.close()

        # Execute pre-batch
        self.exec_pre_batch()

        print "[DBI] All logs will be in the directory: ",self.log_dir
        # Launch bqsubmit
        if not self.test:
            for t in self.tasks:
                t.set_scheduled_time()
            self.p = Popen( 'bqsubmit', shell=True)
            self.p.wait()
            
            if self.p.returncode!=0:
                raise DBIError("[DBI] ERROR: bqsubmit returned an error code of"+str(self.p.returncode))
        else:
            print "[DBI] Test mode, we generated all files, but will not execute bqsubmit"
            if self.dolog:
                print "[DBI] The scheduling time will not be logged when you submit the generated file"

        # Execute post-batchs
        self.exec_post_batch()

    def wait(self):
        print "[DBI] WARNING cannot wait until all jobs are done for bqtools, use bqwatch or bqstatus"


###############################
# Sun Grid Engine
# (used on CLUMEQ's colosse
###############################

class DBISge(DBIBase):
    def __init__(self, commands, **args):
        self.jobs_name = ''
        self.queue = ''
        self.duree = '23:59:59'
        self.project = 'jvb-000-aa'
        self.env = ''
        self.set_special_env = True
        DBIBase.__init__(self, commands, **args)

        self.tmp_dir = os.path.abspath(self.tmp_dir)
        self.log_dir = os.path.abspath(self.log_dir)
        if not self.jobs_name:
            #self.jobs_name = os.path.split(self.log_dir)[1]
            self.jobs_name = 'dbi_'+self.unique_id[1:12]
        ## No TMP_DIR needed for the moment
        ##if not os.path.exists(self.tmp_dir):
        ##    os.makedirs(self.tmp_dir)
        ##print "[DBI] All SGE file will be in ", self.tmp_dir
        ##os.chdir(self.tmp_dir)
        self.args = args
        self.add_commands(commands)

        # Warn for not implemented features
        if getattr(self, 'nb_proc', -1) != -1:
            sge_root = os.getenv("SGE_ROOT")
            if not sge_root:
                print "[DBI] WARNING: DBISge need sge 6.2u4 or higher to work for nb_proc!=-1 to work. Colosse have 6.2u3", self.nb_proc
            elif os.path.split(sge_root)[1].startswith('ge'):
                if os.path.split(sge_root)[1][2:]<'6.2u4':
                    print "[DBI] WARNING: DBISge need sge 6.2u4 or higher to work for nb_proc!=-1 to work. We found version '%s' to be running."%(sge_root[2:]), self.nb_proc
            else:
                print "[DBI] WARNING: DBISge need sge 6.2u4 or higher to work for nb_proc!=-1 to work. Can't determine the version of sge that is running.", self.nb_proc
            #print "[DBI] WARNING: DBISge need sge 6.2u4 or higher to work for nb_proc!=-1 to work. Colosse have 6.2u3", self.nb_proc



    def add_commands(self,commands):
        if not isinstance(commands, list):
            commands=[commands]

        # create the information about the tasks
        for command in commands:
            id=len(self.tasks)+1
            self.tasks.append(Task(
                command = command,
                tmp_dir = self.tmp_dir,
                log_dir = self.log_dir,
                time_format = self.time_format,
                pre_tasks = self.pre_tasks,
                post_tasks = self.post_tasks,
                dolog = self.dolog,
                id = id,
                gen_unique_id = False,
                args = self.args))
            id+=1

    def run(self):
        pre_batch_command = ';'.join( self.pre_batch )
        post_batch_command = ';'.join( self.post_batch )

        launcher = open(os.path.join(self.log_dir, 'launcher'), 'w')
        launcher.write(dedent('''\
                #!/bin/bash -l
                # Bash is needed because we use its "array" data structure
                # the -l flag means it will act like a login shell,
                # and source the .profile, .bashrc, and so on

                # List of all tasks to execute
                tasks=(
                '''))
        for task in self.tasks:
            launcher.write("'" + ';'.join(task.commands) + "'\n")
        launcher.write(dedent('''\
                )

                # The index in 'tasks' array starts at 0,
                # but SGE_TASK_ID starts at 1...
                ID=$(($SGE_TASK_ID - 1))

                ## Trap SIGUSR1 and SIGUSR2, so the job has time to react
                # These signals are emitted by SGE before (respectively)
                # SIGSTOP and SIGKILL (typically 60 s before on colosse)
                trap "echo signal trapped by $0 >&2" SIGUSR1 SIGUSR2

                # Execute the task
                ${tasks[$ID]}
                '''))

        submit_sh_template = '''\
                #!/bin/bash

                ## Reasonable default values
                # Execute the job from the current working directory.
                #$ -cwd
                # Send "warning" signals to a running job prior to sending the signals themselves. 
                #$ -notify

                ## Mandatory arguments
                #Specifies  the  project (RAPI number from CCDB) to  which this job is assigned.
                #$ -P %(project)s
                #All jobs must be submitted with an estimated run time
                #$ -l h_rt=%(duree)s

                ## Job name
                #$ -N %(name)s

                ## log out/err files
                #$ -o %(log_dir)s/$JOB_NAME.$JOB_ID.$TASK_ID.log.out
                #$ -e %(log_dir)s/$JOB_NAME.$JOB_ID.$TASK_ID.log.err

                ## Trap SIGUSR1 and SIGUSR2, so the job has time to react
                # These signals are emitted by SGE before (respectively)
                # SIGSTOP and SIGKILL (typically 60 s before on colosse)
                trap "echo signal trapped by $0 >&2" SIGUSR1 SIGUSR2

                ## Execute as many jobs as needed
                #$ -t 1-%(n_tasks)i:1
                '''
        if self.cpu > 0:
            submit_sh_template += '''
                ## Number of CPU (on the same node) per job
                #$ -pe smp %(cpu)i
                '''
        if self.mem > 0:
            submit_sh_template += '''
                ## Memory size (on the same node) per job
                #$ -l ml=%sM
                '''%str(self.mem)
            
        if self.queue:
            submit_sh_template += '''
                ## Queue name
                #$ -q %(queue)s
                '''

        if self.nb_proc>0:
            submit_sh_template += '''
                ## Maximum of concurrent jobs need sge 6.2u4 or more recent.
                #$ -tc %s
                '''%self.nb_proc

        env = self.env
        if self.set_special_env and self.cpu>0:
            if not env:
                env = '""'
            env += ' OMP_NUM_THREADS=%d GOTO_NUM_THREADS=%d MKL_NUM_THREADS=%d'%(self.cpu,self.cpu,self.cpu)
        if env:
            submit_sh_template += '''
                ## Variable to put into the environment
                #$ -v %s
                '''%(','.join(env.split()))

        if self.raw:
            submit_sh_template += '''%s
                '''%self.raw

        submit_sh_template += '''
                ## Execute the 'launcher' script in bash
                # Bash is needed because we use its "array" data structure
                # the -l flag means it will act like a login shell,
                # and source the .profile, .bashrc, and so on
                /bin/bash -l -e %(log_dir)s/launcher
                '''

        submit_sh = open(os.path.join(self.log_dir, 'submit.sh'), 'w')
        submit_sh.write(dedent(
            submit_sh_template % dict(
                project = self.project,
                duree = self.duree,
                name = self.jobs_name,
                log_dir = self.log_dir,
                n_tasks = len(self.tasks),
                cpu = self.cpu,
                queue = self.queue,
            )))

        submit_sh.close()

        # Execute pre-batch
        self.exec_pre_batch()

        print "[DBI] All logs will be in the directory: ", self.log_dir
        print "[DBI] WARNING: the log formatting specified by --task_names will be ignored,"
        print "     the following format will be used: $JOB_NAME.$JOB_ID.$TASK_ID.log.{err,out}"
        # Launch qsub
        submit_command = 'qsub ' + os.path.join(self.log_dir, 'submit.sh')
        if not self.test:
            for t in self.tasks:
                t.set_scheduled_time()

            self.p = Popen(submit_command, shell=True)
            self.p.wait()

            if self.p.returncode!=0:
                raise DBIError("[DBI] ERROR: qsub returned an error code of"+str(self.p.returncode))
        else:
            print "[DBI] Test mode, we generated all files, but will not execute qsub"
            print '[DBI] Test mode, to manually launch it execute "'+submit_command+'"'
            
            if self.dolog:
                print "[DBI] The scheduling time will not be logged when you submit the generated file"

        # Execute post-batchs
        self.exec_post_batch()

    def clean(self):
        pass

    def wait(self):
        print "[DBI] WARNING cannot wait until all jobs are done for SGE, use qstat or qmon(need X11)"



###################
# Sharcnet tools
###################

class DBISharcnet(DBIBase):
    def __init__(self, commands, **args):
        self.jobs_name = ''
        self.queue = ''
        self.duree = '7d'

        #TODO:
        # self.env
        # self.set_special_env

        DBIBase.__init__(self, commands, **args)

        self.tmp_dir = os.path.abspath(self.tmp_dir)
        self.log_dir = os.path.abspath(self.log_dir)
        if not self.jobs_name:
            self.jobs_name = 'dbi_'+self.unique_id[1:12]

        self.tmp_dir = os.path.join(self.tmp_dir, os.path.split(self.log_dir)[1])
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        print "[DBI] All temporary files will be in ", self.tmp_dir
        os.chdir(self.tmp_dir)

        args['tmp_dir'] = self.tmp_dir
        self.args = args
        self.add_commands(commands)

    def add_commands(self, commands):
        if not isinstance(commands, list):
            commands=[commands]

        # create the information about the tasks
        for command in commands:
            id = len(self.tasks) + 1
            self.tasks.append(Task(
                command = command,
                tmp_dir = self.tmp_dir,
                log_dir = self.log_dir,
                time_format = self.time_format,
                pre_tasks = self.pre_tasks,
                post_tasks = self.post_tasks,
                dolog = self.dolog,
                id = id,
                gen_unique_id = False,
                args = self.args))
            id += 1

    def run_one_job(self, task):
        DBIBase.run(self)

        remote_command = string.join(task.commands, '\n')
        filename = os.path.join(self.tmp_dir, task.unique_id)
        filename = os.path.abspath(filename)
        f = open(filename, 'w')
        f.write(remote_command)
        f.write('\n')
        f.close()
        os.chmod(filename, 0750)
        self.temp_files.append(filename)

        command = 'sqsub'
        (output_file, error_file)=self.get_file_redirection(task.id)
        command += ' -o ' + output_file
        command += ' -e ' + error_file
        if self.cpu > 0:
            command += ' -n ' + str(self.cpu)
        if self.mem > 0:
            command += ' --mpp=' + str(self.mem) + 'M' # The suffix is needed by sqsub
        if self.duree:
            command += ' -r ' + self.duree
        if self.queue:
            command += ' -q ' + self.queue
        if self.jobs_name:
            command += ' -j ' + self.jobs_name
        if self.gpu:
            # TODO: support several GPUs per job?
            command += ' --gpp=1'

        command += " '" + filename + "'"

        if not self.test:
            task.set_scheduled_time()

            self.p = Popen(command, shell=True)
            self.p.wait()
            if self.p.returncode != 0:
                raise DBIError("[DBI] ERROR: sqsub returned an error code of"+str(self.p.returncode))
        else:
            print '[DBI] Test mode, to manually submit, execute "'+command+'"'


    def run(self):
        print "[DBI] The log files are under %s" % self.log_dir
        if self.test:
            "[DBI] Test mode, we generated all files, but will not execute sqsub"

        pre_batch_command = ';'.join(self.pre_batch)
        post_batch_command = ';'.join(self.post_batch)

        # Execute pre-batch
        self.exec_pre_batch()

        for t in self.tasks:
            self.run_one_job(t)

        # Execute post-batch
        self.exec_post_batch()

    def clean(self):
        return
        for f in self.temp_files:
            os.remove(f)

    def wait(self):
        print "[DBI] WARNING cannot wait until all jobs are done on Sharcnet, use sqjobs or sqstat"


# Transfor a string so that it is treated by Condor as a single argument
def condor_escape_argument(argstring):
    # Double every single quote and double quote character,
    # surround the result by a pair of single quotes,
    # then surrount everything by a pair of double quotes
    return "\"'" + argstring.replace("'", "''").replace('"','""') + "'\""

def condor_dag_escape_argument(argstring):
    # escape the double quote so that dagman handle it corretly
    # DAGMAN don't handle single quote!!!
    if "'" in argstring:
        raise DBIError("[DBI] ERROR: the condor back-end with dagman don't support using the ' symbol in the command to execute")
    if ";" in argstring:
        raise DBIError("[DBI] ERROR: the condor back-end with dagman don't support the symbol ';' in the command to execute!")
    return argstring.replace('"',r'\\\"')

class DBICondor(DBIBase):

    def __init__( self, commands, **args ):
        self.getenv = False
        self.nice = False
        # in Meg for initialization for consistency with cluster
        # then in kilo as that is what is needed by condor
        self.req = ''
        self.rank = ''
        self.copy_local_source_file = False
        self.files = ''
        self.file_redirect_stdout = False
        self.file_redirect_stderr = False
        self.redirect_stderr_to_stdout = False
        self.env = ''
        self.os = ''
        self.abs_path = True
        self.set_special_env = True
        self.nb_proc = -1 # < 0   mean unlimited
        self.source_file = ''
        self.source_file = os.getenv("CONDOR_LOCAL_SOURCE")
        self.condor_home = os.getenv('CONDOR_HOME')
        self.condor_submit_exec = "condor_submit"
        self.condor_submit_dag_exec = "condor_submit_dag"
        self.pkdilly = False
        self.launch_file = None
        self.universe = "vanilla"
        self.machine = []
        self.machines = []
        self.no_machine = []
        self.to_all = False
        self.keep_failed_jobs_in_queue = False
        self.clean_up = True
        self.max_file_size = 10*1024*1024 #in blocks size, here they are 1k each
        self.debug = False
        self.local_log_file = True#by default true as condor can have randomly failure otherwise.
        self.next_job_start_delay = -1
        self.imagesize=-1

        DBIBase.__init__(self, commands, **args)
        if self.debug:
            self.condor_submit_exec+=" -debug"
            self.condor_submit_dag_exec+=" -debug"
        valid_universe = ["standard", "vanilla", "grid", "java", "scheduler", "local", "parallel", "vm"]
        if not self.universe in valid_universe:
            raise DBIError("[DBI] ERROR: the universe option have an invalid value",self.universe,". Valid values are:",valid_universe)
        if self.universe=="local":
            n=subprocess.Popen("cat /proc/cpuinfo |grep processor|wc -l", shell = True, stdout=PIPE).stdout.readline()
            if len(commands)>int(n):
                raise DBIError("[DBI] ERROR we refuse to start more jobs on the local universe then the total number of core. Start less jobs or use another universe.")

        if not self.os:
            #if their is not required os, condor launch on the same os.
            p=Popen( "condor_config_val OpSyS", shell=True, stdout=PIPE, stderr=PIPE)
            p.wait()
            out=p.stdout.readlines()
            err=p.stderr.readlines()
            if len(err)!=0 or p.returncode!=0:
                raise Exception("Can't find the os code used by condor on this computer.\n Is condor installed on this computer?\n return code=%d, \n%s"%(p.returncode,"\n".join(err)))
            self.os=out[0].strip()
        else: self.os = self.os.upper()
        
        if not os.path.exists(self.log_dir):
            os.mkdir(self.log_dir) # condor log are always generated

        if not os.path.exists(self.tmp_dir):
            os.mkdir(self.tmp_dir)
        self.args = args

        if self.env and self.env[0]=='"' and self.env[-1]=='"':
            self.env = self.env[1:-1]

        self.next_job_start_delay=int(self.next_job_start_delay)
        self.add_commands(commands)

    def add_commands(self,commands):
        if not isinstance(commands, list):
            commands=[commands]

        # create the information about the tasks
        id=len(self.tasks)+1
        for command in commands:
            c_split = command.split()
            # c = program name, c2 = arguments
            c = c_split[0]
            if len(c_split) > 1:
                c2 = ' ' + ' '.join(c_split[1:])
            else:
                c2 = ''

            # We use the absolute path so that we don't have corner case as with ./
            shellcommand=False
            # Maybe the command is not in the form: executable_name args,
            # but starts with a character that is interpreted by the shell
            # in a special way. I.e., a sequence of commands, like:
            # 'prog1; prog2 arg1 arg2' (with the quotes).
            # The command might also be a shell built-in command.
            # Feel free to complete this list
            shell_special_chars = ["'", '"', ' ', '$', '`', '(', ';']
            authorized_shell_commands=[ "touch", "echo", "cd" ]
            if c[0] in shell_special_chars or c in authorized_shell_commands:
                shellcommand=True
            elif not self.files and self.abs_path:
                # Transform path to get an absolute path.
                c_abs = os.path.abspath(c)
                if os.path.isfile(c_abs):
                    # The file is in the current directory (easy case).
                    c = c_abs
                elif not os.path.isabs(c):
                    # We need to find where the file could be... easiest way to
                    # do it is ask the 'which' shell command.
                    which_out = subprocess.Popen('which %s' % c, shell = True, stdout = PIPE).stdout.readlines()
                    if len(which_out) == 1:
                        c = which_out[0].strip()

            command = "".join([c,c2])

                # We will execute the command on the specified architecture
                # if it is specified. If the executable exist for both
                # architecture we execute on both. Otherwise we execute on the
                # same architecture as the architecture of the launch computer
            self.cplat = get_condor_platform()
            if self.arch == "32":
                self.targetcondorplatform='INTEL'
                newcommand=command
            elif self.arch == "64":
                self.targetcondorplatform='X86_64'
                newcommand=command
            elif self.arch == "3264":
                #the same executable will be executed on all computer
                #So it should be a 32 bits executable
                self.targetcondorplatform='BOTH'
                newcommand=command
            elif c.endswith('.32'):
                self.targetcondorplatform='INTEL'
                newcommand=command
            elif c.endswith('.64'):
                self.targetcondorplatform='X86_64'
                newcommand=command
            elif os.path.exists(c+".32") and os.path.exists(c+".64"):
                self.targetcondorplatform='BOTH'
                #newcommand=c+".32"+c2
                newcommand='if [ $CPUTYPE == \'x86_64\' ]; then'
                newcommand+='  '+c+'.64'+c2
                newcommand+='; else '
                newcommand+=c+".32"+c2+'; fi'
                if not os.access(c+".64", os.X_OK):
                    raise DBIError("[DBI] ERROR: The command '"+c+".64' does not have execution permission!")
#                newcommand=command
                c+=".32"
            elif self.cplat=="INTEL" and os.path.exists(c+".32"):
                self.targetcondorplatform='INTEL'
                c+=".32"
                newcommand=c+c2
            elif self.cplat=="X86_64" and os.path.exists(c+".64"):
                self.targetcondorplatform='X86_64'
                c+=".64"
                newcommand=c+c2
            else:
                self.targetcondorplatform=self.cplat
                newcommand=command

            if shellcommand:
                pass
            elif not os.path.exists(c):
                if not os.path.abspath(c):
                    raise DBIError("[DBI] ERROR: The command '"+c+"' does not exist!")
            elif not os.access(c, os.X_OK):
                raise DBIError("[DBI] ERROR: The command '"+c+"' does not have execution permission!")

            self.tasks.append(Task(newcommand, self.tmp_dir, self.log_dir,
                                   self.time_format, self.pre_tasks,
                                   self.post_tasks,self.dolog,id,False,
                                   self.args))
            id+=1
            #keeps a list of the temporary files created, so that they can be deleted at will

    def get_pkdilly_var(self, out):

#the ssh is to have a renewed and cleaned kerberos ticket
#the +P is to have only the KRV* var, 
#the +P don't need a condor_submit_file
#ssh HOSTNAME pkdilly +P

        cmd="pkdilly -S "+self.condor_submit_file
        self.p = Popen( cmd, shell=True, stdout=PIPE, stderr=PIPE)
        self.p.wait()
        l = self.p.stdout.readline()
        if l!="":
            DBIError("pkdilly returned something on the stdout, this should not happen:\n"+l+"\n"+self.p.stdout.readlines())
        if self.p.returncode!=0:
            DBIError("pkdilly returned an error code of "+str(self.p.returncode)+":\n"+self.p.stderr.readlines()+"\n"+self.p.stdout.readlines())

#example de sortie de pkdilly
#La tache a soumettre est dans: /tmp/soumet_12368_Qbr7Av
        pkdilly_file=""
        for err in self.p.stderr.readlines():
            if err.startswith('La tache a soumettre est dans: '):
                pkdilly_file = err.split()[-1]
        if not pkdilly_file:
            raise DBIError("[DBI] ERROR: pkdilly didn't returned a good string")

        pkdilly_fd = open( pkdilly_file, 'r' )
        lines = pkdilly_fd.readlines()
        pkdilly_fd.close()
        if self.clean_up:
            os.remove(pkdilly_file)
        else:
            self.temp_files.append(pkdilly_file)

        get=[]
        for line in lines:
            if get and line.rfind('"')>=0:
                tmp= line.split('"')[0].strip()
                if tmp:
                    get.append(tmp)
                del get[0]
                break#we got all the env variable...
            elif get:
                get.append(line.strip()[:-1])
            elif line.startswith("environment"):
                get.append(line.strip()[:-1])
        get=[x for x in get if x.startswith("KRV")]
        get=[x for x in get if not x.startswith("KRVEXECUTE=")]
        if out and len(lines)==0:
            out.write("We didnt found kerberos ticket!")
        if out:
            out.write(str(lines))
        return get

    def renew_launch_file(self, renew_out_file,
                          bash_exec, seconds=3600):
        def line_header():
            return "[DBICondor] "+str(datetime.datetime.now())+" "+str(os.getpid())+" "
        
        cmd="condor_wait -wait "+str(seconds)+" "+self.condor_wait_file
        pid=os.fork()
        if pid==0:#in the childreen
            #renew each hour
            out=open(renew_out_file,"w")
            out.write(line_header()+"will renew the lauch file "+self.launch_file+" each "+str(seconds)+"s\n")
            out.flush()
            found=False
            for i in range(5):
                if os.path.isfile(self.log_file):
                    found=True
                    break
                #we do this as in some case(with dagman) the log file can 
                #take a few seconds to be created. So we let it enought time to create it.
                time.sleep(15)
            if not found:
                out.write("Could not found the log file "+self.log_file+"."
                          +"Probably that condor_submit failed.\n")
                out.close()
                sys.exit()
            while True:
                p = Popen( cmd, shell=True, stdout=out, stderr=STDOUT)
                ret = p.wait()
                assert ret==p.returncode
                out.write(line_header()+"condor_wait return code "+
                          str(ret)+"\n")
                if ret==0:
                    out.write(line_header()+
                              "all condor jobs finished. Exiting\n")
                    break
                elif ret!=1:
                    #condor_wait should return only 0 or 1
                    out.write(line_header()+
                              "expected a return code of 0 or 1. Exiting\n")
                    break
                else:
                    s=os.stat(self.launch_file)[os.path.stat.ST_SIZE]
                    out.write(line_header()+
                              "renew the launch file. The old version had a size of "+str(s)+"\n")
                    out.flush()
                    launch_tmp_file=self.launch_file+".tmp"
                    fd=open(launch_tmp_file,'w')
                    kerb_vars=self.make_kerb_script(fd, self.second_lauch_file, 3, out)
                    fd.close()
                    if len(kerb_vars)>0:
                        os.chmod(launch_tmp_file, 0755)
                        os.rename(launch_tmp_file, self.launch_file)
                        s=os.stat(self.launch_file)[os.path.stat.ST_SIZE]
                        out.write(line_header()+
                                  "generated "+str(len(kerb_vars))+" kerberos variables. The file size is "+str(s)+".\n")
                    else:
                        out.write("We have not been able to renew kerberos ticket! Their is 0 kerberos variables!")
                        
                out.flush()
                #we do this as in some case(with dagman) the log file can 
                #take a few seconds to be created. So we don't loop too fast
                #for no good reason.
                time.sleep(60)
            out.close()
            sys.exit()
        else:
            #parent, we pkboost the childreen in case we connect with ssh 
            # then log out. Not sure if this is really need or not.
            
            os.system("pkboost +d "+str(pid))

    def make_kerb_script(self, fd, second_lauch_file, nb_try=3, out=None):
        for i in range(nb_try):
            ##we try 3 times to get the keys as sometimes this fail.                                                    
            vars=self.get_pkdilly_var(out)
            if len(vars)>0:
                break
        if len(vars)==0:
            print "We didn't got any kerberos ticket after %d try! We don't redo the kerberos script."%(nb_try)
            return vars

        fd.write(dedent('''\
                    #!/bin/sh
                    '''))
            
        for g in vars:
            fd.write("export "+g+"\n")
        fd.write(dedent('''
                export KRVEXECUTE=%s
                /usr/sbin/circus "$@"
                '''%(os.path.abspath(second_lauch_file))))
        return vars

    def make_launch_script(self, bash_exec):
            
        #we write in a temp file then move it to be sure no jobs will 
        # read a partially writed file when we renew the file.

        dbi_file=get_plearndir()+'/python_modules/plearn/parallel/dbi.py'
        overwrite_launch_file=False
        if not os.path.exists(dbi_file):
            print '[DBI] WARNING: Can\'t locate file "dbi.py". Maybe the file "'+self.launch_file+'" is not up to date!'
        else:
            if os.path.exists(self.launch_file):
                mtimed=os.stat(dbi_file)[8]
                mtimel=os.stat(self.launch_file)[8]
                if mtimed>mtimel:
                    print '[DBI] WARNING: We overwrite the file "'+self.launch_file+'" with a new version. Update it to your needs!'
                    overwrite_launch_file=True
        if self.pkdilly:
            overwrite_launch_file = True
                    
        if self.copy_local_source_file:
            source_file_dest = os.path.join(self.log_dir,
                                            os.path.basename(self.source_file))
            shutil.copy( self.source_file, source_file_dest)
            self.temp_files.append(source_file_dest)
            os.chmod(source_file_dest, 0755)
            self.source_file=source_file_dest

        launch_tmp_file=self.launch_file+".tmp"
        if not os.path.exists(self.launch_file) or overwrite_launch_file:
            self.temp_files.append(self.launch_file)
            fd = open(launch_tmp_file,'w')
            
            if self.pkdilly:
                self.second_lauch_file = self.launch_file+"2.sh"
                kerb_vars=self.make_kerb_script(fd, self.second_lauch_file)
                fd.close()
                if len(kerb_vars)==0:
                    DBIError("We didn't got kerberos ticket!")

                fd = open(self.second_lauch_file,'w')

            bash=not self.source_file or not self.source_file.endswith(".cshrc")
            if bash:
                fd.write(dedent('''\
                    #!/bin/bash
                    '''))
                if self.condor_home:
                    fd.write('export HOME=%s\n' % self.condor_home)
                fd.write(dedent('''
                    cd %s
                    '''%(os.path.abspath("."))))
                if self.source_file:
                    #we do the next line in hope to remove transiant error to
                    #access this file by the nfs server.
                    fd.write('[ -r "%s" ];echo "Can read the source file? " $? 1>&2 \n'%self.source_file)
                    fd.write('source ' + self.source_file + '\n')

                fd.write(dedent('''\
                    /usr/kerberos/bin/klist
                    echo "Executing on " `/bin/hostname` 1>&2
                    echo "HOSTNAME: ${HOSTNAME}" 1>&2
                    echo "PATH: $PATH" 1>&2
                    echo "PYTHONPATH: $PYTHONPATH" 1>&2
                    echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH" 1>&2
                    echo "OMP_NUM_THREADS: $OMP_NUM_THREADS" 1>&2
                    echo "CONDOR_JOB_LOGDIR: $CONDOR_JOB_LOGDIR" 1>&2
                    echo "HOME: $HOME" 1>&2
                    pwd 1>&2
                    echo "nb args: $#" 1>&2
                    echo "Running: command: \\"$@\\"" 1>&2
                    [ -x "$1" ];echo "Can execute the cmd? " $? 1>&2 
                    %s
                    ret=$?
                    rm -f echo ${KRB5CCNAME:5}
                    echo "return value ${ret}"
                    exit ${ret}
                    '''%(bash_exec)))
            else:
                fd.write(dedent('''\
                    #!/bin/tcsh
                    '''))
                if self.condor_home:
                    fd.write('setenv HOME %s\n' % self.condor_home)
                fd.write('''
                    cd %s
                    '''%(os.path.abspath(".")))
                if self.source_file:
                    fd.write('source ' + self.source_file + '\n')

                fd.write(dedent('''\
                /usr/kerberos/bin/klist
                echo "Executing on " `/bin/hostname`
                echo "HOSTNAME: ${HOSTNAME}"
                echo "PATH: $PATH"
                echo "PYTHONPATH: $PYTHONPATH"
                echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
                echo "OMP_NUM_THREADS: $OMP_NUM_THREADS"
                echo "CONDOR_JOB_LOGDIR: $CONDOR_JOB_LOGDIR"
                echo "HOME: $HOME"
                pwd
                echo "Running command: $argv"
                $argv
                set ret=$?
                rm -f `echo  $KRB5CCNAME| cut -d':' -f2`
                echo "return value ${ret}"
                exit ${ret}
                '''))
            fd.close()
            if self.pkdilly:
                os.chmod(self.second_lauch_file, 0755)

            os.chmod(launch_tmp_file, 0755)
            os.rename(launch_tmp_file, self.launch_file)

    def print_common_condor_submit(self, fd, output, error, arguments=None):
        #check that their is some host with those requirement
        cmd="""condor_status -const '%s' -tot |wc"""%self.req
        p=Popen( cmd, shell=True,stdout=PIPE)
        p.wait()
        lines=p.stdout.readlines()
        if p.returncode != 0 or lines==['      1       0       1\n']:
            raise DBIError("Their is no compute node with those requirement: %s."%self.req)


        fd.write( dedent('''\
                executable     = %s
                universe       = %s
                requirements   = %s
                output         = %s
                error          = %s
                log            = %s
                getenv         = %s
                nice_user      = %s
                ''' % (self.launch_file, self.universe, self.req,
                       output,
                       error,
                       self.log_file,str(self.getenv),str(self.nice))))
        if arguments:
            fd.write('arguments      = '+arguments+'\n')
        if self.keep_failed_jobs_in_queue:
            fd.write('leave_in_queue = (ExitCode!=0)\n')
        if self.next_job_start_delay>0:
            fd.write('next_job_start_delay = %s\n'%self.next_job_start_delay)
        if self.imagesize>0:
            #condor need value in Kb
            fd.write('ImageSize      = %d\n'%(self.imagesize))#need to be in k.

        if self.files: #ON_EXIT_OR_EVICT
            fd.write( dedent('''\
                when_to_transfer_output = ON_EXIT
                should_transfer_files   = Yes
                transfer_input_files    = %s
                '''%(self.files+','+self.launch_file+','+self.tasks[0].commands[0].split()[0]))) # no directory
        if self.env:
            fd.write('environment    = "'+self.env+'"\n')
        if self.raw:
            fd.write( self.raw+'\n')
        if self.rank:
            fd.write( dedent('''\
                rank = %s
                ''' %(self.rank)))

        if self.mem>0:
            fd.write(dedent("""
            request_memory = %i
            """)%(self.mem))
        if self.cpu>0:
            fd.write(dedent("""
            request_cpus = %i
            """)%(self.cpu))

        if self.pkdilly:
            fd.write(dedent("""
            stream_error            = True
            stream_output           = True
            transfer_executable     = True
            when_to_transfer_output = ON_EXIT
            """))
        
    def run_dag(self):
        if self.to_all:
            raise DBIError("[DBI] ERROR: condor backend don't support the option --to_all and a maximum number of process")

        condor_submit_fd = open( self.condor_submit_file, 'w' )

        self.print_common_condor_submit(condor_submit_fd, "$(stdout)", "$(stderr)","$(args)")
        
        condor_submit_fd.write("\nqueue\n")
        condor_submit_fd.close()

        condor_dag_file = self.condor_submit_file+".dag"
        condor_dag_fd = open( condor_dag_file, 'w' )

        def print_task(id, task, stdout_file, stderr_file):
            argstring =condor_dag_escape_argument(' ; '.join(task.commands))
            condor_dag_fd.write("JOB %d %s\n"%(id,self.condor_submit_file))
            self.check_path(stdout_file)
            self.check_path(stderr_file)
            condor_dag_fd.write('VARS %d args="%s"\n'%(id,argstring))
            condor_dag_fd.write('VARS %d stdout="%s"\n'%(id,stdout_file))
            condor_dag_fd.write('VARS %d stderr="%s"\n\n'%(id,stderr_file))
            
        for i in range(len(self.tasks)):
            task=self.tasks[i]
            print_task(i,task,*self.get_file_redirection(task.id))

        condor_dag_fd.close()

        self.make_launch_script('$@')
        time.sleep(5)#we do this in hope that the error 'launch.sh2.sh is not executable' disapear

        condor_cmd = self.condor_submit_dag_exec+' -maxjobs %s %s'%(str(self.nb_proc), condor_dag_file)
        return condor_cmd

    def run_non_dag(self):
        condor_datas = []

        #we supose that each task in tasks have the same number of commands
        #it should be true.
        #NOT IN DAG VERSION
        if len(self.tasks[0].commands)>1:
            for task in self.tasks:
                condor_data = os.path.join(self.tmp_dir,self.unique_id +'.'+ task.unique_id + '.data')
                condor_datas.append(condor_data)
                self.temp_files.append(condor_data)
                param_dat = open(condor_data, 'w')

                param_dat.write( dedent('''\
                #!/bin/bash
                %s''' %('\n'.join(task.commands))))
                param_dat.close()


        condor_submit_fd = open( self.condor_submit_file, 'w' )
            
        #DIFFER IN DAG VERSION
        #self.print_common_condor_submit(condor_submit_fd, "$(stdout)", "$(stderr)","$(args)")
        self.print_common_condor_submit(condor_submit_fd, self.log_dir+"/$(Process).out", self.log_dir+"/$(Process).error")

        if len(condor_datas)!=0:
            for i in condor_datas:
                condor_submit_fd.write("arguments      = sh "+i+" $$(Arch) \nqueue\n")
        else:
            def print_task(task, stdout_file, stderr_file,req=""):
                argstring = condor_escape_argument(' ; '.join(task.commands))
                condor_submit_fd.write("arguments    = %s \n" %argstring)
                self.check_path(stdout_file)
                self.check_path(stderr_file)
                if stdout_file:
                    condor_submit_fd.write("output       = %s \n" %stdout_file)
                if stderr_file:
                    condor_submit_fd.write("error        = %s \nqueue\n" %stderr_file)
                if req:
                    condor_submit_fd.write("requirements   = %s\n"%(req))

            for i in range(len(self.tasks)):
                task=self.tasks[i]
                req=self.tasks_req[i]
                (o,e)=self.get_file_redirection(task.id)
                print_task(task,o,e,req)

        condor_submit_fd.close()

        self.make_launch_script('sh -c "$@"')
        time.sleep(5)#we do this in hope that the error 'launch.sh2.sh is not executable' disapear

        return self.condor_submit_exec + " " + self.condor_submit_file

    def clean(self):
        if len(self.temp_files)>0:
            sleep(20)
            for file_name in self.temp_files:
                try:
                    os.remove(file_name)
                except os.error:
                    pass
                pass

    def run(self):
        if (self.stdouts and not self.stderrs) or (self.stderrs and not self.stdouts):
            raise DBIError("[DBI] ERROR: the condor back-end should have both stdouts and stderrs or none of them")
        if self.stdouts and self.stderrs:
            assert len(self.stdouts)==len(self.stderrs)==len(self.tasks)
            for (stdout_file,stderr_file) in zip(self.stdouts, self.stderrs):
                if stdout_file==stderr_file:
                    raise DBIError("[DBI] ERROR: the condor back-end can't redirect the stdout and stderr to the same file!")

        print "[DBI] The Log file are under %s"%self.log_dir
        if self.source_file and self.source_file.endswith(".cshrc"):
            self.launch_file = os.path.join(self.log_dir, 'launch.csh')
        else:
            self.launch_file = os.path.join(self.log_dir, 'launch.sh')

        self.exec_pre_batch()

        #set special environment variable
        if len(self.tasks)==0:
            return #no task to run

        if self.set_special_env:
            self.env += ' OMP_NUM_THREADS=$$(CPUS) GOTO_NUM_THREADS=$$(CPUS) MKL_NUM_THREADS=$$(CPUS)'

        self.env += ' CONDOR_JOB_LOGDIR=%s'%self.log_dir

        if not self.req:
            self.req = "True"
        if self.targetcondorplatform == 'BOTH':
            self.req+="&&((Arch == \"INTEL\")||(Arch == \"X86_64\"))"
        else :
            self.req+="&&(Arch == \"%s\")"%(self.targetcondorplatform)
        if self.cpu>0:
            self.req+='&&(target.CPUS>='+str(self.cpu)+')'
        if self.mem>0:
            self.req+='&&(Memory>='+str(self.mem)+')'#Must be in Meg
        if self.os:
            self.req=reduce(lambda x,y:x+' || (OpSys == "'+str(y)+'")',
                            self.os.split(','),
                            self.req+'&&(False ')+")"
        machine_choice=[]
        self.tasks_req=[""]*len(self.tasks)
        if not self.to_all:
            #we don't put them in the requirement here
            #as they will be "local" requirement to each jobs.
            for m in self.machine:
                machine_choice.append('(Machine=="'+m+'")')
        else:
            assert(len(self.machines)==0)
            for m in self.machine:
                self.tasks_req.append(self.req+'&&(Machine=="'+m+'")')
        
        for m in self.machines:
            machine_choice.append('regexp("'+m+'", Machine)')

        if len(machine_choice)==1:
            self.req+="&&("+machine_choice[0]+")"
        elif machine_choice:
            self.req+="&&(False "
            for m in machine_choice:
                self.req+="||"+m
            self.req+=")"

        for m in self.no_machine:
            self.req+='&&(Machine!="'+m+'")'
        #if no mem requirement added, use the executable size.
        #todo: if they are not the same executable, take the biggest
        try:
            self.imagesize = os.stat(self.tasks[0].commands[0].split()[0]).st_size/1024
        except:
            pass

        self.condor_submit_file = os.path.join(self.log_dir,
                                               "submit_file.condor")
        self.temp_files.append(self.condor_submit_file)

        if self.local_log_file or self.pkdilly:
            if os.path.exists("/Tmp"):
                self.log_file = "/Tmp"
            else:
                self.log_file = "/tmp"
            self.log_file = os.path.join(self.log_file,os.getenv("USER"),"dbidispatch",self.log_dir)
            os.system('mkdir -p ' + self.log_file)
            self.log_file = os.path.join(self.log_file,"condor.log")
        else:
            self.log_file = os.path.join(self.log_dir,"condor.log")

        #exec dependent code
        if self.nb_proc > 0:
            cmd=self.run_dag()
            self.condor_wait_file = self.condor_submit_file+".dag.dagman.log"
        else:
            cmd=self.run_non_dag()
            self.condor_wait_file = self.log_file

        #add file if needed?
        #why are they needed?
        utils_file = os.path.join(self.tmp_dir, 'utils.py')
        if not os.path.exists(utils_file):
            shutil.copy( get_plearndir()+
                         '/python_modules/plearn/parallel/utils.py', utils_file)
            self.temp_files.append(utils_file)
            os.chmod(utils_file, 0755)

        configobj_file = os.path.join(self.tmp_dir, 'configobj.py')
        if not os.path.exists('configobj.py'):
            shutil.copy( get_plearndir()+
                         '/python_modules/plearn/parallel/configobj.py',  configobj_file)
            self.temp_files.append(configobj_file)
            os.chmod(configobj_file, 0755)

            
        #launch the jobs
        if self.test == False:
            print "[DBI] Executing: " + cmd
            for task in self.tasks:
                task.set_scheduled_time()
            self.p = Popen( cmd, shell=True)
            self.p.wait()
            if self.p.returncode != 0:
                print "[DBI] submission failed! We can't stard the jobs"
            if self.pkdilly:
                self.renew_launch_file(os.path.join(self.log_dir,"renew.outerr")
                                       , 'sh -c "$@"')

        else:
            print "[DBI] In test mode we don't launch the jobs. To do it,",
            print " you need to execute '"+cmd+"'"
            if self.dolog:
                print "[DBI] The scheduling time will not be logged when you will submit the condor file"
            if self.pkdilly:
                print "[DBI] we won't renew the kerberos ticket.",
                print " So the jobs must their execution in the next 8 hours."
        self.exec_post_batch()

    def wait(self):
        print "[DBI] WARNING no waiting for all job to finish implemented for condor, use 'condor_q' or 'condor_wait %s'"%(self.condor_wait_file)

    def clean(self):
        pass

class DBILocal(DBIBase):

    def __init__( self, commands, **args ):
        self.nb_proc=1
        DBIBase.__init__(self, commands, **args)
        self.args=args
        self.threads=[]
        self.mt = None
        self.started=0
        try:
            self.nb_proc=int(self.nb_proc)
        except ValueError,e:
            self.nb_proc_file = self.nb_proc
            f = open(self.nb_proc_file)
            self.nb_proc = int(f.readlines()[0])
            f.close()

        self.add_commands(commands)

    def add_commands(self,commands):
        if not isinstance(commands, list):
            commands=[commands]

        #We copy the variable localy as an optimisation for big list of commands
        #save around 15% with 100 commands
        tmp_dir=self.tmp_dir
        log_dir=self.log_dir
        time_format=self.time_format
        pre_tasks=self.pre_tasks
        post_tasks=self.post_tasks
        dolog=self.dolog
        args=self.args
        id=len(self.tasks)+1
        for command in commands:
            pos = string.find(command,' ')
            if pos>=0:
                c = command[0:pos]
                c2 = command[pos:]
            else:
                c=command
                c2=""

            # We use the absolute path so that we don't have corner case as with ./
            c = os.path.normpath(os.path.join(os.getcwd(), c))
            command = "".join([c,c2])

            # We will execute the command on the specified architecture
            # if it is specified. If the executable exist for both
            # architecture we execute on both. Otherwise we execute on the
            # same architecture as the architecture of the launch computer

            if not os.access(c, os.X_OK):
                raise DBIError("[DBI] ERROR: The command '"+c+"' does not exist or does not have execution permission!")
            self.tasks.append(Task(command, tmp_dir, log_dir,
                                   time_format, pre_tasks,
                                   post_tasks,dolog,id,False,self.args))
            id+=1
        #keeps a list of the temporary files created, so that they can be deleted at will

    def run_one_job(self,task):
        c = (';'.join(task.commands))
        task.set_scheduled_time()

        if self.test:
            print "[DBI] "+c
            return

        (output,error)=self.get_redirection(*self.get_file_redirection(task.id))

        self.started+=1#Is this atomic?
        print "[DBI,%d/%d,%s] %s"%(self.started,len(self.tasks),time.ctime(),c)
        p = Popen(c, shell=True,stdout=output,stderr=error)
        p.wait()
        task.status=STATUS_FINISHED

    def clean(self):
        if len(self.temp_files)>0:
            sleep(20)
            for file_name in self.temp_files:
                try:
                    os.remove(file_name)
                except os.error:
                    pass
                pass

    def run(self):
        if self.test:
            print "[DBI] Test mode, we only print the command to be executed, we don't execute them"
        if not self.file_redirect_stdout and self.nb_proc>1:
            print "[DBI] WARNING: many process but all their stdout are redirected to the parent"
        elif not self.file_redirect_stdout and self.nb_proc_file:
            print "[DBI] WARNING: nb process dynamic with one thread and their stdout are redirected to the parent. Don't change to more then 1 thread!"
        if not self.file_redirect_stderr and self.nb_proc>1:
            print "[DBI] WARNING: many process but all their stderr are redirected to the parent"
        elif not self.file_redirect_stderr and self.nb_proc_file:
            print "[DBI] WARNING: nb process dynamic with one thread and their stderr are redirected to the parent. Don't change to more then 1 thread!"
        print "[DBI] The Log file are under %s"%self.log_dir

        # Execute pre-batch
        self.exec_pre_batch()

        # Execute all Tasks (including pre_tasks and post_tasks if any)
        nb_proc = self.nb_proc
        if self.nb_proc_file:
            nb_proc = self.nb_proc_file
        self.mt=MultiThread(self.run_one_job,self.tasks,nb_proc,lambda :("[DBI,%s]"%time.ctime()))
        self.mt.start()

        #TODO: Need to wait before post_bach?

        # Execute post-batchs
        self.exec_post_batch()


    def clean(self):
        pass

    def wait(self):
        if self.mt:
            try:
                self.mt.join()
            except KeyboardInterrupt, e:
                print "[DBI] Catched KeyboardInterrupt"
                self.print_jobs_status()
                print "[DBI] The Log file are under %s"%self.log_dir
                raise
        else:
            print "[DBI] WARNING jobs not started!"
        self.print_jobs_status()
        print "[DBI] The Log file are under %s"%self.log_dir

class SshHost:
    def __init__(self, hostname,nice=19,get_avail=True):
        self.hostname= hostname
        self.minupdate=15
        self.lastupd= -1-self.minupdate
        self.working=True
        (self.bogomips,self.ncores,self.loadavg)=(-1.,-1,-1.)
        self.nice=nice
        if get_avail:
            self.getAvailability()

    def getAvailability(self):
        # simple heuristic: mips / load
        t= time.time()
        if t - self.lastupd > self.minupdate: # min. 15 sec. before update
            (self.bogomips,self.ncores,self.loadavg)=self.getAllHostInfo()
            self.lastupd= t
            #print  self.hostname, self.bogomips, self.loadavg, (self.bogomips / (self.loadavg + 0.5))
        return self.bogomips / (self.loadavg + 0.5)

    def getAllHostInfo(self):
        cmd= ["ssh", self.hostname ,"cat /proc/cpuinfo;cat /proc/loadavg"]
        p= Popen(cmd, stdout=PIPE)
        bogomips= -1
        ncores=-1
        loadavg=-1
        returncode = p.returncode
        wait = p.wait()
        if returncode:
            self.working=False
            return (-1.,-1,-1.)
        elif wait!=0:
            self.working=False
            return (-1.,-1,-1.)

        for l in p.stdout:
            if l.startswith('bogomips'):
                s= l.split(' ')
                bogomips+= float(s[-1])
            if l.startswith('processor'):
                s= l.split(' ')
                ncores=int(s[-1])+1

        if l:
            loadavg=float(l[0])
        #(bogomips,ncores,load average)
        return (bogomips,ncores,loadavg)

    def addToLoadavg(self,n):
        self.loadavg+= n
        self.lastupd= time.time()

    def __str__(self):
        return "SshHost("+self.hostname+" <nice: "+str(self.nice)\
               +"bogomips:"+str(self.bogomips)\
               +',ncores:'+str(self.ncores)\
               +',loadavg'+str(self.loadavg)\
               +',avail:'+str(self.getAvailability())\
               +',lastupd:'+str(self.lastupd) + '>)'

    def __repr__(self):
        return str(self)

def get_hostname():
    from socket import gethostname
    myhostname = gethostname()
    pos = string.find(myhostname,'.')
    if pos>=0:
        myhostname = myhostname[0:pos]
    return myhostname

# copied from PLearn/python_modules/plearn/pymake/pymake.py
def get_platform():
    #should we use an env variable called PLATFORM???
    #if not defined, use uname uname -i???
    pymake_osarch = os.getenv('PYMAKE_OSARCH')
    if pymake_osarch:
        return pymake_osarch
    platform = sys.platform
    if platform=='linux2':
        linux_type = os.uname()[4]
        if linux_type == 'ppc':
            platform = 'linux-ppc'
        elif linux_type =='x86_64':
            platform = 'linux-x86_64'
        else:
            platform = 'linux-i386'
    return platform

# copied from PLearn/python_modules/plearn/pymake/pymake.py
def find_all_ssh_hosts():
    hostspath_list = [os.path.join(os.getenv("HOME"),".pymake",get_platform()+'.hosts')]
    if os.path.exists(hostspath_list[0])==0:
        raise DBIError("[DBI] ERROR: no host file %s for the ssh backend"%(hostspath_list[0]))
    print "[DBI] using file %s for the list of host"%(hostspath_list[0])
#    from plearn.pymake.pymake import process_hostspath_list
#    (list_of_hosts, nice_values) = process_hostspath_list(hostspath_list,19,get_hostname())
    shuffle(list_of_hosts)
    print list_of_hosts
    print nice_values
    h=[]
    for host in list_of_hosts:
        print "connecting to",host
        s=SshHost(host,nice_values[host],False)
        if s.working:
            h.append(s)
        else:
            print "[DBI] host not working:",s.hostname
        print s
    print h
    return h

def cmp_ssh_hosts(h1, h2):
    return cmp(h2.getAvailability(), h1.getAvailability())

class DBISsh(DBIBase):

    def __init__(self, commands, **args ):
        print "[DBI] WARNING: The SSH DBI is not fully implemented!"
        print "[DBI] Use at your own risk!"
        self.nb_proc=1
        DBIBase.__init__(self, commands, **args)
        self.args=args
        self.add_commands(commands)
        self.hosts= find_all_ssh_hosts()
        print "[DBI] hosts: ",self.hosts

    def add_commands(self,commands):
        if not isinstance(commands, list):
            commands=[commands]

        # create the information about the tasks
        id=len(self.tasks)+1
        for command in commands:
            self.tasks.append(Task(command, self.tmp_dir, self.log_dir,
                                   self.time_format, self.pre_tasks,
                                   self.post_tasks,self.dolog,id,False,
                                   self.args))
            id+=1

    def getHost(self):
        self.hosts.sort(cmp= cmp_ssh_hosts)
        print "hosts= "
        for h in self.hosts: print h
        self.hosts[0].addToLoadavg(1.0)
        return self.hosts[0]

    def run_one_job(self, task):
        DBIBase.run(self)

        host= self.getHost()

        cwd= os.getcwd()
        command = "ssh " + host.hostname + " 'cd " + cwd + "; " + string.join(task.commands,';') + "'"
        print "[DBI] "+command

        if self.test:
            return

        task.launch_time = time.time()
        task.set_scheduled_time()
        (output,error)=self.get_redirection(*self.get_file_redirection(task.id))

        task.p = Popen(command, shell=True,stdout=output,stderr=error)
        task.p.wait()
        task.status=STATUS_FINISHED


    def run_one_job2(self, host):
        DBIBase.run(self)

        cwd= os.getcwd()
        print self._locked_iter
        for task in self._locked_iter:
            print "task",task
            command = "ssh " + host.hostname + " 'cd " + cwd + "; " + string.join(task.commands,';') + " ; echo $?'"
            print "[DBI, %s] %s"%(time.ctime(),command)

            if self.test:
                return

            task.launch_time = time.time()
            task.set_scheduled_time()


            task.p = Popen(command, shell=True,stdout=PIPE,stderr=PIPE)
            wait = task.p.wait()
            returncode = p.returncode
            if returncode:
                self.working=False

            elif wait!=0:
                self.working=False
                #redo it
            return -1.

            out=task.p.stdout.readlines()
            err=task.p.stderr.readlines()
            self.echo_result=None
            if out:
                self.echo_result=int(out[-1])
                del out[-1]
            print "out",out
            print "err",err
            print "echo result",self.echo_result
            if err:
                task.return_status = int(err[-1])  # last line was an echo $? (because rsh doesn't transmit the status byte correctly)
                del err[-1]
                print "return status", task.return_status
            sleep(1)
            task.status=STATUS_FINISHED

    def run(self):
        print "[DBI] The Log file are under %s"%self.log_dir
        if not self.file_redirect_stdout and self.nb_proc>1:
            print "[DBI] WARNING: many process but all their stdout are redirected to the parent"
        if not self.file_redirect_stderr and self.nb_proc>1:
            print "[DBI] WARNING: many process but all their stderr are redirected to the parent"

        # Execute pre-batch
        self.exec_pre_batch()
        self._locked_iter=LockedListIter(iter(self.tasks))
        if self.test:
            print "[DBI] In testmode, we only print the command that would be executed."
        print "in run",self.hosts
        # Execute all Tasks (including pre_tasks and post_tasks if any)
        self.mt=MultiThread(self.run_one_job2,self.hosts,self.nb_proc,lambda :("[DBI,%s]"%time.ctime()))
        self.mt.start()

        # Execute post-batchs
        self.exec_post_batch()

    def clean(self):
        #TODO: delete all log files for the current batch
        pass

    def wait(self):
        #TODO
        self.mt.join()
        self.print_jobs_status()


# creates an object of type ('DBI' + launch_system) if it exists
def DBI(commands, launch_system, **args):
    """The Distributed Batch Interface is a collection of python classes
    that make it easy to execute commands in parallel using different
    systems like condor, bqtools on Mammouth, the cluster command or localy.
    """
    try:
        jobs = eval('DBI'+launch_system+'(commands,**args)')
    except DBIError, e:
        print e
        sys.exit(1)
    except NameError:
        print 'The launch system ',launch_system, ' does not exists. Available systems are: Cluster, Ssh, Bqtools and Condor'
        traceback.print_exc()
        sys.exit(1)
    return jobs

def main():
    if len(sys.argv)!=2:
        print "Usage: %s {Condor|Cluster|Ssh|Local|Bqtools} < joblist"%(sys.argv[0])
        print "Where joblist is a file containing one experiment on each line"
        sys.exit(0)
    DBI([ s[0:-1] for s in sys.stdin.readlines() ], sys.argv[1]).run()
#    jobs.clean()

#    config['LOG_DIRECTORY'] = 'LOGS/'
if __name__ == "__main__":
    main()
