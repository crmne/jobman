
import os
import os.path
import glob

from tools import DD
import parse
from sql import RUNNING, DONE

from optparse import OptionParser
from runner import runner_registry

def sync_single_directory(dir_path, force=False):
    conf = DD(parse.filemerge(os.path.join(dir_path, 'current.conf')))

    if not conf.has_key('jobman.status') \
       or not conf.has_key('jobman.sql.host_workdir') \
       or not conf.has_key('jobman.sql.host_name'):
        print "abort for", dir_path, " because at least one of jobman.status,", \
                "jobman.sql.host_workdir or jobman.sql.host_name is not specified."
        return

    if conf['jobman.status'] != RUNNING:
        if force and conf['jobman.status'] == DONE:
            print "sync forced for complete job", dir_path
        else:
            print "won't sync", dir_path, "as job is not running (no sync to do)"
            return

    perform_sync(dir_path, conf)

def perform_sync(dir_path, conf):
    remote_dir = conf['jobman.sql.host_workdir']
    remote_host = conf['jobman.sql.host_name']

    # we add a trailing slash, otherwise it'll create
    # the directory on destination
    if remote_dir[-1] != "/":
        remote_dir += "/"

    host_string = remote_host + ":" + remote_dir

    rsync_command = 'rsync -ac "%s" "%s"' % (host_string, dir_path)

    os.system(rsync_command)

def sync_all_directories(base_dir, force=False):
    oldcwd = os.getcwd()
    os.chdir(base_dir)

    all_dirs = glob.glob("*/current.conf")

    if len(all_dirs) == 0:
        print "No subdirectories containing a file named 'current.conf' found."

    os.chdir(oldcwd)

    for dir_and_file in all_dirs:
        dir, file = dir_and_file.split("/")

        full_path = os.path.join(base_dir, dir)

        sync_single_directory(full_path, force)

def cachesync_runner(options, dir):
    """
    Syncs the working directory of jobs with remote cache.

    Usage: cachesync [options] <path_to_job(s)_workingdir(s)>

    (For this to work, though, you need to do a channel.save() at least
    once in your job before calling cachesync, otherwise the host_name
    and host_workdir won't be set in current.conf)

    It can either sync a single directory, which must contain "current.conf"
    file which specifies the remote host and directory. Example for a single
    directory:

        # this syncs the current directory
        jobman cachesync .

        # this syncs another directory
        jobman cachesync myexperiment/mydbname/mytablename/5

    It can also sync all subdirectories of the directory you specify.
    You must use the -m (or --multiple) option for this.
    Each subdirectory (numbered 1, 2 ... etc based on job number) must
    contain a "current.conf" file specifying the remote host and directory.
    Examples:

        # syncs all subdirectories 1, 2 ...
        jobman cachesync -m myexperiment/mydbname/mytablename 

    Normally completed jobs (status = DONE) won't be synced based on
    the "status" set in current.conf. Yet you can force sync by using
    the -f or --force option.
    """
    force = options.force
    multiple = options.multiple

    if multiple:
        sync_all_directories(dir, force)
    else:
        sync_single_directory(dir, force)

################################################################################
### register the command
################################################################################

cachesync_parser = OptionParser(
    usage = '%prog cachesync [options] <path_to_job(s)_workingdir(s)>',
    add_help_option=False)
cachesync_parser.add_option('-f', '--force', dest = 'force', default = False, action='store_true',
                              help = 'force rsync even if the job is complete')
cachesync_parser.add_option('-m', '--multiple', dest = 'multiple', default = False, action='store_true',
                               help = 'sync multiple jobs (in that case, "path_to_job" must be the directory that contains all the jobs, i.e. its subdirectories are 1, 2, 3...)')

runner_registry['cachesync'] = (cachesync_parser, cachesync_runner)

