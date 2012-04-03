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
import inspect

import errors
import colour
import config

##############################################################################
# GLOBAL DEFNITIONS
##############################################################################
site_to_telescope = {'i': 'WSRT',
                     'wt': 'WSRT',
                     'wsrt': 'WSRT',
                     'westerbork': 'WSRT',
                     'g': 'Effelsberg', 
                     'ef': 'Effelsberg',
                     'eff': 'Effelsberg',
                     'effelsberg': 'Effelsberg',
                     '8': 'Jodrell',
                     'jb': 'Jodrell',
                     'jbo': 'Jodrell',
                     'jodrell bank': 'Jodrell',
                     'jodrell bank observatory': 'Jodrell',
                     'lovell': 'Jodrell',
                     'f': 'Nancay',
                     'nc': 'Nancay',
                     'ncy': 'Nancay',
                     'nancay': 'Nancay',
                     'sardinia': 'SRT',
                     'srt': 'SRT'}

telescope_to_dir = {'Jodrell': 'jb', \
                    'WSRT': 'wsrt', \
                    'SRT': 'srt', \
                    'Effelsberg': 'eff', \
                    'Nancay': 'ncy'}

header_param_types = {'freq': float, \
                      'length': float, \
                      'bw': float, \
                      'mjd': float, \
                      'intmjd': int, \
                      'fracmjd': float, \
                      'backend': str, \
                      'rcvr': str, \
                      'telescop': str, \
                      'name': str, \
                      'nchan': int, \
                      'npol': int, \
                      'nbin': int, \
                      'nsub': int, \
                      'tbin': float}


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
        print_debug("Successfully connected to database %s.%s as %s"%(Host,DBname,Username), 'database')
    except OperationalError:
        print "Could not connect to database!"
        raise
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
        errors.FileError("File %s does not exist, you dumb dummy!" % file)
    elif verbose:
        print "File %s exists!" % file

    #Determine path (will retrieve absolute path)
    file_path, file_name = os.path.split(os.path.abspath(file))
    if verbose:
        print "Path: %s Filename: %s" % (file_path, file_name)
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


def get_userids(cursor=None):
    """Return a dictionary mapping user names to user ids.

        Input:
            cursor: Cursor to connect to DB.
                (Default: Establish a connection and use that cursor).

        Output:
            userids: A dictionary with user names as keys 
                    and user ids as values.
    """
    if cursor is None:
        # Create DB connection instance
        DBcursor, DBconn = DBconnect()
    else:
        DBcursor = cursor
    
    query = "SELECT user_name, user_id FROM users"
    DBcursor.execute(query)

    rows = DBcursor.fetchall()
    if cursor is None:
        # Close the DB connection we opened
        DBconn.close()

    # Create the mapping
    userids = {}
    for uname, uid in rows:
        userids[uname] = uid
    return userids


def get_pulsarids(cursor=None):
    """Return a dictionary mapping pulsar names to pulsar ids.

        Input:
            cursor: Cursor to connect to DB.
                (Default: Establish a connection and use that cursor).

        Output:
            pulsarids: A dictionary with pulsar names as keys
                    and pulsar ids as values.
    """
    if cursor is None:
        # Create DB connection instance
        DBcursor, DBconn = DBconnect()
    else:
        DBcursor = cursor
    
    query = "SELECT pulsar_name, " \
                "pulsar_jname, " \
                "pulsar_bname, " \
                "pulsar_id " \
            "FROM pulsars"
    DBcursor.execute(query)

    rows = DBcursor.fetchall()
    if cursor is None:
        # Close the DB connection we opened
        DBconn.close()

    # Create the mapping
    pulsarids = {}
    for name, jname, bname, id in rows:
        trimname = name.lower().lstrip('bj')
        pulsarids[trimname] = id
        pulsarids[name] = id
        pulsarids[jname] = id
        pulsarids[bname] = id
    return pulsarids


def get_obssystemids(cursor=None):
    """Return a dictionary mapping fronend/backend combinations
        to obs system ids.

        Input:
            cursor: Cursor to connect to DB.
                (Default: Establish a connection and use that cursor).

        Output:
            obssystemids: A dictionary with a (frontend, backend) tuple as keys
                    and obs system ids as values.
    """
    if cursor is None:
        # Create DB connection instance
        DBcursor, DBconn = DBconnect()
    else:
        DBcursor = cursor
    
    query = "SELECT t.name, " \
                "o.frontend, " \
                "o.backend, " \
                "o.obssystem_id " \
            "FROM obssystems AS o " \
            "LEFT JOIN telescopes AS t " \
                "ON t.telescope_id = o.telescope_id"
    DBcursor.execute(query)

    rows = DBcursor.fetchall()
    if cursor is None:
        # Close the DB connection we opened
        DBconn.close()

    # Create the mapping
    obssystemids = {}
    for telescope, frontend, backend, id in rows:
        obssystemids[(telescope, frontend, backend)] = id
    return obssystemids


def get_telescope(site):
    """Given a site identifier return the telescope's name. 
        Possible identifiers are:
        - telescope name
        - 1-char site code
        - 2-char site code -
        - telescope abbreviation
        
        Input:
            site: String to identify site.
                    (Idenfier is not case sensitive)

        Output:
            telescope: Name of the telescope.
    """
    site = site.lower()
    if site not in site_to_telescope:
        raise errors.UnrecognizedValueError("Site identifier (%s) " \
                                            "is not recognized" % site)
    return site_to_telescope[site]


def get_header_vals(fn, hdritems):
    """Get a set of header params from the given file.
        Returns a dictionary.

        Inputs:
            fn: The name of the file to get params for.
            hdritems: List of parameters (recognized by vap) to fetch.

        Output:
            params: A dictionary. The keys are values requested from 'vap'
                the values are the values reported by 'vap'.
    """
    hdrstr = ",".join(hdritems)
    if '=' in hdrstr:
        raise ValueError("'hdritems' passed to 'get_header_vals' " \
                         "should not perform and assignments!")
    cmd = "vap -n -c '%s' %s" % (hdrstr, fn)
    outstr, errstr = execute(cmd)
    outvals = outstr.split()[1:] # First value is filename (we don't need it)
    if errstr:
        raise errors.SystemCallError("The command: %s\nprinted to stderr:\n%s" % \
                                (cmd, errstr))
    elif len(outvals) != len(hdritems):
        raise errors.SystemCallError("The command: %s\nreturn the wrong " \
                            "number of values. (Was expecting %d, got %d.)" % \
                            (cmd, len(hdritems), len(outvals)))
    params = {}
    for key, val in zip(hdritems, outvals):
        if val == "INVALID":
            raise errors.SystemCallError("The vap header key '%s' " \
                                            "is invalid!" % key)
        elif val == "*" or val == "UNDEF":
            warnings.warn("The vap header key '%s' is not " \
                            "defined in this file (%s)" % (key, fn), \
                            errors.EptaPipelineWarning)
            params[key] = None
        else:
            # Get param's type to cast value
            caster = header_param_types.get(key, str)
            params[key] = caster(val)
    return params


def parse_psrfits_header(fn, hdritems):
    """Get a set of header params from the given file.
        Returns a dictionary.

        Inputs:
            fn: The name of the file to get params for.
            hdritems: List of parameter names to fetch.

        Output:
            params: A dictionary. The keys are values requested from 'psredit'
                the values are the values reported by 'psredit'.
    """
    hdrstr = ",".join(hdritems)
    if '=' in hdrstr:
        raise ValueError("'hdritems' passed to 'parse_psrfits_header' " \
                         "should not perform and assignments!")
    cmd = "psredit -q -Q -c '%s' %s" % (hdrstr, fn)
    outstr, errstr = execute(cmd)
    outvals = outstr.split()
    if errstr:
        raise errors.SystemCallError("The command: %s\nprinted to stderr:\n%s" % \
                                (cmd, errstr))
    elif len(outvals) != len(hdritems):
        raise errors.SystemCallError("The command: %s\nreturn the wrong " \
                            "number of values. (Was expecting %d, got %d.)" % \
                            (cmd, len(hdritems), len(outvals)))
    params = {}
    for key, val in zip(hdritems, outstr.split()):
        params[key] = val
    return params
    

def get_archive_dir(fn, site=None, backend=None, psrname=None):
    """Given a file name return where it should be archived.

        Input:
            fn: The name of the file to archive.
            site: Value of "site" keyword from 'psredit'.
                Providing this will override the value stored
                in the file header.
                (Default: Fetch value using 'psredit'.)
            backend: Name of backend as reported by 'psredit'.
                Providing this will override the value stored
                in the file header.
                (Default: Fetch value using 'psredit'.)
            psrname: Name of the pulsar as reported by 'psredit'.
                Providing this will override the value stored
                in the file header.
                (Default: Fetch value using 'psredit'.)

        Output:
            dir: The directory where the file should be archived.
    """
    if (site is None) or (backend is None) or (psrname is None):
        params_to_get = ['site', 'be:name', 'name']
        params = parse_psrfits_header(fn, params_to_get)
        if site is None:
            site = params['site']
        if backend is None:
            backend = params['be:name']
        if psrname is None:
            psrname = params['name']
    sitedir = telescope_to_dir[get_telescope(site)]
    
    dir = os.path.join(config.data_archive_location, sitedir.lower(), \
                        backend.lower(), psrname)
    return dir


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


def print_info(msg, level=1):
    """Print an informative message if the current verbosity is
        higher than the 'level' of this message.

        The message will be colourized as 'info'.

        Inputs:
            msg: The message to print.
            level: The verbosity level of the message.
                (Default: 1 - i.e. don't print unless verbosity is on.)

        Outputs:
            None
    """
    if config.verbosity >= level:
        if config.excessive_verbosity:
            # Get caller info
            fn, lineno, funcnm = inspect.stack()[1][1:4]
            colour.cprint("INFO (level: %d) [%s:%d - %s(...)]:" % 
                    (level, os.path.split(fn)[-1], lineno, funcnm), 'infohdr')
            msg = msg.replace('\n', '\n    ')
            colour.cprint("    %s" % msg, 'info')
        else:
            colour.cprint(msg, 'info')


def print_debug(msg, category):
    """Print a debugging message if the given debugging category
        is turned on.

        The message will be colourized as 'debug'.

        Inputs:
            msg: The message to print.
            category: The debugging category of the message.

        Outputs:
            None
    """
    if config.debug.is_on(category):
        if config.helpful_debugging:
            # Get caller info
            fn, lineno, funcnm = inspect.stack()[1][1:4]
            to_print = colour.cstring("DEBUG %s [%s:%d - %s(...)]:\n" % \
                        (category.upper(), os.path.split(fn)[-1], lineno, funcnm), \
                            'debughdr')
            msg = msg.replace('\n', '\n    ')
            to_print += colour.cstring("    %s" % msg, 'debug')
        else:
            to_print = colour.cstring(msg, 'debug')
        sys.stderr.write(to_print + '\n')
        sys.stderr.flush()


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
        group.add_argument('-v', '--more-verbose', nargs=0, \
                            action= self.TurnUpVerbosity, \
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
    
    class TurnUpVerbosity(argparse.Action):
        def __call__(self, parse, namespace, values, option_string):
            config.verbosity += 1

    class SetDebugMode(argparse.Action): 
        def __call__(self, parser, namespace, values, option_string):
            mode = option_string.split("--debug-")[1].upper()
            config.debug.set_mode_on(mode)

    class SetAllDebugModes(argparse.Action): 
        def __call__(self, parser, namespace, values, option_string):
            config.debug.set_allmodes_on()
