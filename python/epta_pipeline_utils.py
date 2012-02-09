#!/usr/bin/python2.6
################################
# epta_pipeline_utils.py    
# Useful, general functions 
################################

#Imported modules

import sys
from sys import argv, exit
from os import system, popen
from MySQLdb import *
import os.path
import datetime
import argparse
import hashlib
import subprocess
import types

import errors
import config
##############################################################################
# Functions
##############################################################################

def DBconnect(Host=config.dbhost, DBname=config.dbname, \
                Username=config.dbuser, Password=config.dbpass, \
                cursor_class=cursors.Cursor):
    #To make a connection to the database
    try:
        connection = connect(host=Host,db=DBname,user=Username,passwd=Password)
        cursor = connection.cursor(cursor_class)
        print "Successfully connected to database %s.%s as %s"%(Host,DBname,Username)
    except OperationalError:
        print "Could not connect to database!  Exiting..."
        exit(0)
    return cursor, connection
                    
def Run_python_script(script, args_list, verbose=0, test=0):
    #Use to run an external python script in the shell
    COMMAND = config.python+" "+script+" "+" ".join("%s" % arg for arg in args_list)
    if verbose:
        print "Running command: "+COMMAND
    if not test:
        system(COMMAND)

def Run_shell_command(command, verbose=0, test=0):
    #Use to run an external program in the shell
    COMMAND = command
    if verbose:
        print "Running command: "+COMMAND
    if not test:
        system(COMMAND)        

def Verify_file_path(file, verbose=0):
    #Verify that file exists
    if not os.path.isfile(file):
        print "File %s does not exist, you dumb dummy!"%(file)
        exit(0)
    elif  os.path.isfile(file) and verbose:
        print "File %s exists!"%(file)
    #Determine path (will retrieve absolute path)
    file_path, file_name = os.path.split(os.path.abspath(file))
    if verbose:
        print "Path: %s Filename: %s"%(file_path, file_name)
    return file_path, file_name

def Fill_pipeline_table(DBcursor,DBconn):
    #Calculate md5sum of pipeline script
    MD5SUM = popen("md5sum %s"%argv[0],"r").readline().split()[0].strip()
    QUERY = "INSERT INTO pipeline (pipeline_name, pipeline_version, md5sum) " \
            "VALUES ('%s','%s','%s')" % (config.pipe_name, config.version, MD5SUM)
    DBcursor.execute(QUERY)
    #Get pipeline_id
    QUERY = "SELECT LAST_INSERT_ID()"
    DBcursor.execute(QUERY)
    pipeline_id = DBcursor.fetchall()[0][0]
    print "Added pipeline name and version to pipeline table with pipeline_id = %s"%pipeline_id
    return pipeline_id
    
def Make_Proc_ID():
    utcnow = datetime.datetime.utcnow()
    return "%d%02d%02d_%02d%02d%02d.%d"%(utcnow.year,utcnow.month,utcnow.day,utcnow.hour,utcnow.minute,utcnow.second,utcnow.microsecond)

def Make_Tstamp():
        utcnow = datetime.datetime.utcnow()
        return "%04d-%02d-%02d %02d:%02d:%02d"%(utcnow.year,utcnow.month,utcnow.day,utcnow.hour,utcnow.minute,utcnow.second)

def Give_UTC_now():
    utcnow = datetime.datetime.utcnow()
    return "UTC %d:%02d:%02d on %d%02d%02d"%(utcnow.hour,utcnow.minute,utcnow.second,utcnow.year,utcnow.month,utcnow.day)

def Get_md5sum(fname, block_size=16*8192):
    """Compute and return the MD5 sum for the given file.
        The file is read in blocks of 'block_size' bytes.

        Inputs:
            fname: The name of the file to get the md5 for.
            block_size: The number of bytes to read at a time.
                (Default: 16*8192)

        Output:
            md5: The hexidecimal string of the MD5 checksum.
    """
    f = open(fname, 'rb')
    md5 = hashlib.md5()
    block = f.read(block_size)
    while block:
        md5.update(block)
        block = f.read(block_size)
    f.close()
    return md5.hexdigest()


def execute(cmd, stdout=subprocess.PIPE, stderr=sys.stderr, dir=None):
    """Execute the command 'cmd' after logging the command
        to STDOUT. Execute the command in the directory 'dir',
        which defaults to the current directory is not provided.

        Output standard output to 'stdout' and standard
        error to 'stderr'. Both are strings containing filenames.
        If values are None, the out/err streams are not recorded.
        By default stdout is subprocess.PIPE and stderr is sent 
        to sys.stderr.

        Returns (stdoutdata, stderrdata). These will both be None, 
        unless subprocess.PIPE is provided.
    """
    # Log command to stdout
    if config.debug.SYSCALLS:
        sys.stdout.write("\n'"+cmd+"'\n")
        sys.stdout.flush()

    stdoutfile = False
    stderrfile = False
    if type(stdout) == types.StringType:
        stdout = open(stdout, 'w')
        stdoutfile = True
    if type(stderr) == types.StringType:
        stderr = open(stderr, 'w')
        stderrfile = True

    # Run (and time) the command. Check for errors.
    pipe = subprocess.Popen(cmd, shell=True, cwd=dir, \
                            stdout=stdout, stderr=stderr)
    (stdoutdata, stderrdata) = pipe.communicate()
    retcode = pipe.returncode
    if retcode < 0:
        raise errors.SystemCallError("Execution of command (%s) terminated by signal (%s)!" % \
                                (cmd, -retcode))
    elif retcode > 0:
        raise errors.SystemCallError("Execution of command (%s) failed with status (%s)!" % \
                                (cmd, retcode))
    else:
        # Exit code is 0, which is "Success". Do nothing.
        pass

    # Close file objects, if any
    if stdoutfile:
        stdout.close()
    if stderrfile:
        stderr.close()

    return (stdoutdata, stderrdata)


class DefaultArguments(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        argparse.ArgumentParser.__init__(self, *args, **kwargs)

    def parse_args(self, *args, **kwargs):
        # Add default groups just before parsing so it is the last set of
        # options displayed in help text
        self.add_standard_group()
        self.add_debug_group()
        return argparse.ArgumentParser.parse_args(self, *args, **kwargs)

    def add_standard_group(self):
        group = self.add_argument_group("Standard Options", \
                    "The following options get used by various programs.")
        group.add_argument('-v', '--more-verbose', action='count', \
                            default=0, dest="verbosity", \
                            help="Be more verbose. (Default: " \
                                 "Don't be verbose at all.)")

    def add_debug_group(self):
        group = self.add_argument_group("Debug Options", \
                    "The following options turn on various debugging " \
                    "statements. Multiple debugging options can be " \
                    "provided.")
        group.add_argument('-d', '--debug', nargs=0, \
                            action=self.SetAllDebugModes, \
                            help="Turn on all debugging modes. (Same as --debug-all).")
        group.add_argument('--debug-all', nargs=0, \
                            action=self.SetDebugMode, \
                            help="Turn on all debugging modes. (Same as -d/--debug).")
        for m, desc in config.debug.modes:
            group.add_argument('--debug-%s' % m.lower(), nargs=0, \
                            action=self.SetDebugMode, \
                            help=desc)

    class SetDebugMode(argparse.Action): 
        def __call__(self, parser, namespace, values, option_string):
            mode = option_string.split("--debug-")[1].upper()
            config.debug.set_mode_on(mode)

    class SetAllDebugModes(argparse.Action): 
        def __call__(self, parser, namespace, values, option_string):
            config.debug.set_allmodes_on()
