#tested this script in ~/test_jobman
#python ~/repos/Jobman/bin/jobman condor_check 'postgres://ift6266h10:f0572cd63b@gershwin.iro.umontreal.ca/ift6266h10_sandbox_db/testing_fred'
#job submitted from maggie46

from subprocess import Popen,PIPE
import os, time
import sql
from optparse import OptionParser
from runner import runner_registry
from tools import UsageError

parse_check_condor = OptionParser(usage = '%prog check_condor <tablepath> ',
                            add_help_option=False)

#parse_check_condor.add_option('', '--restart', action = 'store_true', dest = 'restart', default = False,
#                              help = 'Re schedule a jobs marked as running when we know that it failed.')

def check_condor_serve(options, dbdescr):
    """Check that all jobs marked as running in the db are marked as running in condor

    print jobs that could have crashed.

    Example usage:

        jobman check_condor <tablepath>

    """

    try:
        username, password, hostname, port, dbname, tablename \
            = sql.parse_dbstring(dbdescr)
    except Exception, e:
        raise UsageError('Wrong syntax for dbdescr',e)
    db = sql.postgres_serial(
        user = username,
        password = password,
        host = hostname,
        port = port,
        database = dbname,
        table_prefix = tablename)
    try:
        session = db.session()
        q = db.query(session)
        running = q.filter_eq('jobman.status',1).all()
        info = []
        print "I: Their is %d jobs marked as running in the db"%len(running)
        print
        #check not 2 jobs in same slot+host
        host_slot={}
        now = time.time()
        def str_time(x):
            run_time = now-x
            run_time = "%dd %dh%dm%ds"%(run_time/(24*3600),run_time%(24*3600)/3600,run_time%3600/60,run_time%60)
            return run_time
        for idx,r in enumerate(running):
            h = r["jobman.sql.host_name"]
            s = r["jobman.sql.condor_slot"]
            st = s+'@'+h
            if host_slot.has_key(st):
                print 'E: Job %d and Job %d are running on the same condor slot/host combination. running time: %s and %s'%(running[host_slot[st]].id,r.id,str_time(running[host_slot[st]]["jobman.sql.start_time"]),str_time(r["jobman.sql.start_time"]))
            else: host_slot[st]=idx
            
        #check job still running on condor
        for r in running:
            try: 
                r["jobman.sql.condor_slot"]
            except KeyError:
                #if "jobman.sql.condor_slot" not in r.keys(): #don't work if not all item have that value.
                print "W: Job %d  is running but don't have a condor_slot defined. It could have been started with an old version of jobman."%r.id
                continue
            info = (r.id, r["jobman.experiment"],r["jobman.sql.condor_slot"], r["jobman.sql.host_name"], r["jobman.sql.start_time"])
            run_time = str_time(info[4])
            
            if info[2]=="no_condor_slot":
                print "W: Job %d is not running on condor(Should not happed...)"%info[0]
            else:
                p=Popen('''condor_status -constraint 'Name == "slot%s@%s"' -format "%%s" Name -format " %%s" State -format " %%s" Activity -format " %%s" RemoteUser -format " %%s\n" RemoteOwner'''%(info[2],info[3]),
                        shell=True, stdout=PIPE)
                p.wait()
                lines=p.stdout.readlines()
                #return when running: slot1@brams0b.iro.umontreal.ca Claimed Busy bastienf bastienf
                #return when don't exist: empty
                if len(lines)==0:
                    print "W: Job %d is running on a host that condor lost connection with. The job run for: %s"%(r.id,run_time)
                    continue
                elif len(lines)!=1 and not (len(lines)==2 and lines[-1]=='\n'):
                    print "W: Job %d condor_status return not understood: ",lines
                    continue
                sp = lines[0].split()
                if sp[1]=="Unclaimed" and sp[2]=="Idle":
                        print "E: Job %d db tell that this job is running a job. condor tell that this host don't run a job. running time %s"%(r.id,run_time)
                elif len(lines[0].split())==5:
                    sp = lines[0].split()
                    assert sp[0]=="slot%s@%s"%(info[2],info[3])
                    if sp[3]!=sp[4]:
                        print "W: Job %d condor_status return not understood: ",lines
                    if sp[1]=="Claimed" and sp[2] in ["Busy","Retiring"]:
                        if sp[4].split('@')[0]==os.getenv("USER"):
                            print "W: Job %d is running on a condor host that is running a job of the same user. running time: %s"%(r.id,run_time)
                        else:
                            print "E: Job %d is running on a condor host that is running a job for user %s. running time: %s"%(r.id,sp[4].split('@')[0],run_time)
                    else:
                        print "W: Job %d condor state of host not understood"%r.id,sp
                else:
                    print "W: Job %d condor_status return not understood: ",lines

                
    finally:
        session.close()

runner_registry['condor_check'] = (parse_check_condor, check_condor_serve)
