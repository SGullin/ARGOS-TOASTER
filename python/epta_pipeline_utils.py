#!/usr/bin/python2.6
################################
# epta_pipeline_utils.py    
# Useful, general functions 
################################

#Imported modules

import sys
from MySQLdb import *
import os
import shutil
import os.path
import datetime
import argparse
import hashlib
import subprocess
import types
import inspect
import string
import warnings
import re

import errors
import colour
import config
import database

##############################################################################
# GLOBAL DEFENITIONS
##############################################################################
site_to_telescope = {'i': 'WSRT',
                     'wb': 'WSRT',
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
                     'jodrell': 'Jodrell',
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

# The following regular expressions are used when parse parfiles
float_re = re.compile(r"^[-+]?(\d+(\.\d*)?|\.\d+)([eEdD][-+]?\d+)?$")
int_re = re.compile(r"^[-+]?\d+$")


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
        os.system(COMMAND)

def Run_shell_command(command, verbose=0, test=0):
    #Use to run an external program in the shell
    COMMAND = command
    if verbose:
        print "Running command: "+COMMAND
    if not test:
        os.system(COMMAND)        

def Verify_file_path(file):
    #Verify that file exists
    print_info("Verifying file: %s" % file, 2)
    if not os.path.isfile(file):
        raise errors.FileError("File %s does not exist, you dumb dummy!" % file)

    #Determine path (will retrieve absolute path)
    file_path, file_name = os.path.split(os.path.abspath(file))
    print_info("File %s exists!" % os.path.join(file_path, file_name), 3)
    return file_path, file_name


def Fill_process_table(version_id, rawfile_id, parfile_id, template_id, \
                            cmdline, nchan, nsub, db):
    query = "INSERT INTO process (version_id,rawfile_id,proc_start_time,input_args,parfile_id,template_id,nchan,nsub) VALUES ('%s','%s','%s','%s','%s','%s','%s','%s')" % (version_id,rawfile_id,Make_Tstamp(),string.join(argv," "),parfile_id,template_id,nchan,nsub)
    DBcursor.execute(QUERY)
    QUERY = "SELECT LAST_INSERT_ID()"
    DBcursor.execute(QUERY)
    process_id = DBcursor.fetchall()[0][0]
    print "Added pipeline name and version to pipeline table with pipeline_id = %s"%process_id
    return process_id
    
def Make_Tstamp():
        utcnow = datetime.datetime.utcnow()
        return "%04d-%02d-%02d %02d:%02d:%02d"%(utcnow.year,utcnow.month,utcnow.day,utcnow.hour,utcnow.minute,utcnow.second)

def Give_UTC_now():
    utcnow = datetime.datetime.utcnow()
    return "UTC %d:%02d:%02d on %d%02d%02d"%(utcnow.hour,utcnow.minute,utcnow.second,utcnow.year,utcnow.month,utcnow.day)


def get_userids(existdb=None):
    """Return a dictionary mapping user names to user ids.

        Input:
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            userids: A dictionary with user names as keys 
                    and user ids as values.
    """
    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    query = "SELECT user_name, user_id FROM users"
    db.execute(query)

    rows = db.fetchall()
    if not existdb:
        # Close the DB connection we opened
        db.close()

    # Create the mapping
    userids = {}
    for uname, uid in rows:
        userids[uname] = uid
    return userids


def get_pulsarids(existdb=None):
    """Return a dictionary mapping pulsar names to pulsar ids.

        Input:
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            pulsarids: A dictionary with pulsar names as keys
                    and pulsar ids as values.
    """
    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    query = "SELECT pulsar_name, " \
                "pulsar_jname, " \
                "pulsar_bname, " \
                "pulsar_id " \
            "FROM pulsars"
    db.execute(query)

    rows = db.fetchall()
    if not existdb:
        # Close the DB connection we opened
        db.close()

    # Create the mapping
    pulsarids = {}
    for name, jname, bname, id in rows:
        trimname = name.lower().lstrip('bj')
        pulsarids[trimname] = id
        pulsarids[name] = id
        pulsarids[jname] = id
        pulsarids[bname] = id
    return pulsarids


def get_obssystemids(existdb=None):
    """Return a dictionary mapping fronend/backend combinations
        to obs system ids.

        Input:
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            obssystemids: A dictionary with a (frontend, backend) tuple as keys
                    and obs system ids as values.
    """
    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    query = "SELECT t.name, " \
                "o.frontend, " \
                "o.backend, " \
                "o.obssystem_id " \
            "FROM obssystems AS o " \
            "LEFT JOIN telescopes AS t " \
                "ON t.telescope_id = o.telescope_id"
    db.execute(query)

    rows = db.fetchall()
    if not existdb:
        # Close the DB connection we opened
        db.close()

    # Create the mapping
    obssystemids = {}
    for telescope, frontend, backend, id in rows:
        obssystemids[(telescope.lower(), frontend.lower(), backend.lower())] = id
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
   

def get_pulsar_names(existing_db=None):
    """Return a dictionary mapping pulsar names and ids 
        to preferred pulsar names.
        
        Input:
            existing_db: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            pulsarids: A dictionary with pulsar names as keys
                    and pulsar ids as values.
    """
    # Use the exisitng DB connection, or open a new one if None was provided
    db = existing_db or database.Database()
    query = "SELECT pulsar_name, " \
                "pulsar_jname, " \
                "pulsar_bname, " \
                "pulsar_id " \
            "FROM pulsars"
    db.execute(query)

    rows = db.fetchall()
    if not existing_db:
        # Close the DB connection we opened
        db.close()

    # Create the mapping
    pulsar_names = {}
    for name, jname, bname, id in rows:
        trimname = name.lower().lstrip('bj')
        pulsar_names[trimname] = name
        pulsar_names[bname] = name
        pulsar_names[jname] = name
        pulsar_names[id] = name
    return pulsar_names


def get_archive_dir(fn, data_archive_location=config.data_archive_location, \
                        site=None, backend=None, receiver=None, psrname=None):
    """Given a file name return where it should be archived.

        Input:
            fn: The name of the file to archive.
            data_archive_location: The base directory of the 
                archive. (Default: use location listed in config
                file).
            site: Value of "site" keyword from 'psredit'.
                Providing this will override the value stored
                in the file header.
                (Default: Fetch value using 'vap'.)
            backend: Name of backend as reported by 'vap'.
                Providing this will override the value stored
                in the file header.
                (Default: Fetch value using 'vap'.)
            receiver: Name of receiver as reported by 'vap'.
                Providing this will override the value stored
                in the file header.
                (Default: Fetch value using 'vap'.)
            psrname: Name of the pulsar as reported by 'psredit'.
                Providing this will override the value stored
                in the file header.
                (Default: Fetch value using 'vap'.)

        Output:
            dir: The directory where the file should be archived.
    """
    if (site is None) or (backend is None) or (psrname is None) or \
            (receiver is None):
        params_to_get = ['telescop', 'backend', 'rcvr', 'name']
        params = get_header_vals(fn, params_to_get)
        if site is None:
            site = params['telescop']
        if backend is None:
            backend = params['backend']
        if receiver is None:
            receiver = params['rcvr']
        if psrname is None:
            psrname = get_pulsar_names()[params['name']]
    sitedir = telescope_to_dir[get_telescope(site)]
    
    dir = os.path.join(data_archive_location, psrname, sitedir.lower(), \
                        backend.lower(), receiver.lower())
    return dir


def prep_parfile(fn):
    """Prepare parfile for archiving/loading.
        
        Also, perform some checks on the parfile to make sure we
        won't run into problems later. Checks peformed:
            - Existence of file.
            - Read/write access for file (so it can be moved).

        Input:
            fn: The name of the parfile to check.

        Outputs:
            params: A dictionary of parameters contained in the file.
                NOTE: parameters that look like ints or floats are cast
                    as such.
    """
    # Check existence of file
    Verify_file_path(fn)

    # Check file permissions allow for writing and reading
    if not os.access(fn, os.R_OK):
        raise errors.FileError("File (%s) is not readable!" % fn)

    # Grab parameters from file
    f = open(fn, 'r')
    params = {}
    for line in f.readlines():
        # Ignore blank lines
        line = line.strip()
        if not line:
            continue
        key, valstr = line.split()[:2]

        if float_re.match(valstr):
            # Looks like a float. Change 'D' to 'E' and cast to float.
            val = float(valstr.upper().replace('D','E'))
        elif int_re.match(valstr):
            # Looks like an int. Cast to int.
            val = int(valstr)
        else:
            # Doesn't seem like a number. Leave as string.
            val = valstr

        params[key.lower()] = val
    if "psrj" in params:
        params['pulsar_id'] = get_pulsarids()[params['psrj']]
        params['name'] = params['psrj']
    else:
        params['pulsar_id'] = get_pulsarids()[params['psrb']]
        params['name'] = params['psrb']
    # normalise pulsar name
    params['name'] = get_pulsar_names()[params['name']]
    params['user_id'] = get_current_users_id()
    return params


def prep_file(fn):
    """Prepare file for archiving/loading.
        
        Also, perform some checks on the file to make sure we
        won't run into problems later. Checks peformed:
            - Existence of file.
            - Read/write access for file (so it can be moved).
            - Header contains all necessary values.
            - Site/observing system is recognized.

        Input:
            fn: The name of the file to check.

        Outputs:
            params: A dictionary of info to be uploaded.
    """
    # Check existence of file
    Verify_file_path(fn)

    # Check file permissions allow for writing and reading
    if not os.access(fn, os.R_OK):
        raise errors.FileError("File (%s) is not readable!" % fn)

    # Grab header info
    hdritems = ["nbin", "nchan", "npol", "nsub", "type", "telescop", \
         	"name", "dec", "ra", "freq", "bw", "dm", "rm", \
      	        "dmc", "rm_c", "pol_c", "scale", "state", "length", \
    	        "rcvr", "basis", "backend"]
    params = get_header_vals(fn, hdritems)

    # Get telescope name
    params['telescop'] = get_telescope(params['telescop'])

    # Check if obssystem_id, pulsar_id, user_id can be found
    obssys_key = (params['telescop'].lower(), params['rcvr'].lower(), \
                                params['backend'].lower())
    obssys_ids = get_obssystemids()
    if obssys_key not in obssys_ids:
        t, r, b = obssys_key
        raise errors.FileError("The observing system combination in the file " \
                            "%s is not registered in the database. " \
                            "(Telescope: %s, Receiver: %s; Backend: %s)." % \
                            (fn, t, r, b))
    else:
        params['obssystem_id'] = obssys_ids[obssys_key]
    
    # Check if pulsar_id is found
    psr_ids = get_pulsarids()
    if params['name'] not in psr_ids:
        raise error.FileError("The pulsar name %s (from file %s) is not " \
                            "recognized." % (params['name'], fn))
    else:
        # Normalise pulsar name
        params['name'] = get_pulsar_names()[params['name']]
        params['pulsar_id'] = psr_ids[params['name']]

        params['user_id'] = get_current_users_id()
    return params


def get_current_users_id(existdb=None):
    """Get the user ID of the current user and return it.
        
        Input:
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            user_id: The current user's ID.
    """
    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()

    # Check if user_id is found
    user_ids = get_userids(db)
    username = os.getlogin()
    if username not in user_ids:
        raise errors.FileError("The current user's username (%s) is not " \
                            "registered in the database." % username)
    else:
        user_id = user_ids[username]

    if not existdb:
        # Close the DB connection we opened
        db.close()

    return user_id


def is_gitrepo_dirty(repodir):
    """Return True if the git repository has local changes.

        Inputs:
            repodir: The location of the git repository.

        Output:
            is_dirty: True if git repository has local changes. False otherwise.
    """
    try:
        stdout, stderr = execute("git diff --quiet", dir=repodir)
    except errors.SystemCallError:
        # Exit code is non-zero
        return True
    else:
        # Success error code (i.e. no differences)
        return False


def get_githash(repodir):
    """Get the pipeline's git hash.

        Inputs:
            repodir: The location of the git repository.

        Output:
            githash: The githash
    """
    if is_gitrepo_dirty(repodir):
        warnings.warn("Git repository has uncommitted changes!", \
                        errors.EptaPipelineWarning)
    stdout, stderr = execute("git rev-parse HEAD", dir=repodir)
    githash = stdout.strip()
    return githash


def get_version_id(existdb=None):
    """Get the pipeline version number.
        If the version number isn't in the database, add it.

        Input:
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            version_id: The version ID for the current pipeline/psrchive
                combination.
    """
    # Check to make sure the repositories are clean
    check_repos()
    # Get git hashes
    pipeline_githash = get_githash(config.epta_pipeline_dir)
    psrchive_githash = get_githash(config.psrchive_dir)
    
    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()

    # Check to see if this combination of versions is in the database
    query = "SELECT version_id FROM versions " \
            "WHERE pipeline_githash='%s' AND " \
                "psrchive_githash='%s' AND " \
                "tempo2_cvsrevno='Not available'" % \
            (pipeline_githash, psrchive_githash)
    db.execute(query)
    rows = db.fetchall()
    if len(rows) > 1:
        raise errors.DatabaseError("There are too many (%d) matching " \
                                    "version IDs" % len(rows))
    elif len(rows) == 1:
        version_id = rows[0].version_id
    else:
        # Insert the current versions
        query = "INSERT INTO versions " \
                "SET pipeline_githash='%s', " % pipeline_githash + \
                    "psrchive_githash='%s', " % psrchive_githash + \
                    "tempo2_cvsrevno='Not available'"
        db.execute(query)
        
        # Get the newly add version ID
        query = "SELECT LAST_INSERT_ID()"
        db.execute(query)
        version_id = db.fetchone()[0]
    
    if not existdb:
        # Close the DB connection we opened
        db.close()

    return version_id


def check_repos():
    """Check git repositories for the pipeline code, and for PSRCHIVE.
        If the repos are dirty raise and error.

        Inputs:
            None

        Outputs:
            None
    """
    if epu.is_gitrepo_dirty(config.epta_pipeline_dir):
        if debug.PIPELINE:
            warnings.warn("Git repository is dirty! Will tolerate because " \
                            "pipeline debugging is on.", \
                            errors.EptaPipelineWarning)
        else:
            raise errors.EptaPipelineError("Pipeline's git repository is dirty. Aborting!")

    if epu.is_gitrepo_dirty(config.psrchive_dir):
        raise errors.EptaPipelineError("PSRCHIVE's git repository is dirty. " \
                                        "Clean up your act!")


def archive_file(file, destdir):
    srcdir, fn = os.path.split(file)
    dest = os.path.join(destdir, fn)

    # Check if the directory exists
    # If not, create it
    if not os.path.isdir(destdir):
        # Set permissions (in octal) to read/write/execute for user and group
        print_info("Making directory: %s" % destdir, 2)
        os.makedirs(destdir, 0770)

    # Check that our file doesn't already exist in 'dest'
    # If it does exist do nothing but print a warning
    if not os.path.isfile(dest):
        # Copy file to 'dest'
        print_info("Moving %s to %s" % (file, dest), 2)
        shutil.copy2(file, dest)
        
        # Check that file copied successfully
        srcmd5 = Get_md5sum(file)
        srcsize = os.path.getsize(file)
        destmd5 = Get_md5sum(dest)
        destsize = os.path.getsize(dest)
        if (srcmd5==destmd5) and (srcsize==destsize):
            print_info("File copied successfully to %s. Removing %s." % \
                        (dest, file), 2)
            if not config.debug.ARCHIVING:
                os.remove(file)
        else:
            raise errors.ArchivingError("File copy failed! (Source MD5: %s, " \
                        "Dest MD5: %s; Source size: %d, Dest size: %d)" % \
                        (srcmd5, destmd5, srcsize, destmd5))
    elif destdir == srcdir:
        # File is already located in its destination
        # Do nothing
        warnings.warn("Source file %s is already in the archive (and in " \
                        "the correct place). Doing nothing..." % file, \
                        errors.EptaPipelineWarning)
        pass
    else:
        # Another file with the same name is the destination directory
        # Compare the files
        srcmd5 = Get_md5sum(file)
        srcsize = os.path.getsize(file)
        destmd5 = Get_md5sum(dest)
        destsize = os.path.getsize(dest)
        if (srcmd5==destmd5) and (srcsize==destsize):
            # Files are the same, so remove src as if we moved it
            # (taking credit for work that was already done...)
            warnings.warn("Another version of this file (%s), with " \
                            "the same size (%d bytes) and the same " \
                            "MD5 (%s) is already in the archive. " \
                            "Removing input file..." % \
                            (file, destsize, destmd5), \
                            errors.EptaPipelineWarning)
            if not config.debug.ARCHIVING:
                os.remove(file)
        else:
            # The files are not the same! This is not good.
            # Raise an exception.
            raise errors.ArchivingError("File (%s) cannot be archived. " \
                    "There is already a file archived by that name " \
                    "in the appropriate archive location (%s), but " \
                    "the two files are _not_ identical. " \
                    "(source: MD5=%s, size=%d bytes; dest: MD5=%s, " \
                    "size=%d bytes)" % \
                    (file, dest, srcmd5, srcsize, destmd5, destsize))

    # Change permissions so the file can no longer be written to
    print_info("Changing permissions of archived file to 440", 2)
    os.chmod(dest, 0440) # "0440" is an integer in base 8. It works
                         # the same way 440 does for chmod on cmdline
    return dest


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


def get_master_parfile(pulsar_id):
    """Given a pulsar ID number return the full path
        to that pulsar's master parfile. If no master parfile
        exists return None.

        Input:
            pulsar_id: The pulsar ID number to get a master parfile for.

        Output:
            masterpar_id: The master parfile's parfile_id value, or
                None if no master parfile exists.
            fn: The master parfile's full path, or None if no master
                parfile exists.
    """
    db = database.Database()
    query = "SELECT par.parfile_id, " \
                "par.filepath, " \
                "par.filename " \
            "FROM pulsars AS psr " \
            "LEFT JOIN parfiles AS par " \
                "ON par.parfile_id=psr.master_parfile_id " \
            "WHERE psr.pulsar_id=%d" % pulsar_id
    db.execute(query)
    rows = db.fetchall()
    if len(rows) > 1:
        raise errors.InconsistentDatabaseError("There are too many (%d) " \
                                            "master parfiles for pulsar #%d" % \
                                            (len(rows), pulsar_id ))
    elif len(rows) == 0:
        return None, None
    else:
        masterpar_id, path, fn = rows[0]
        if path is None or fn is None:
            return None, None
        else:
            return masterpar_id, os.path.join(path, fn)


def get_master_template(pulsar_id, obssystem_id):
    """Given a pulsar ID number, and observing system ID number
        return the full path to the appropriate master template, 
        and its ID number. If no master template exists return
        None.

        Inputs:
            pulsar_id: The pulsar ID number.
            obssystem_id: The observing system ID number.

        Outputs:
            mastertmp_id: The master template's template_id value, or
                None if no master template exists for the pulsar/obssystem
                combination provided.
            fn: The master template's full path, or None if no master
                template exists for the provided pulsar/obssystem
                combination.
    """
    db = database.Database()
    query = "SELECT tmp.tempate_id, " \
                "tmp.filename, " \
                "tmp.filepath " \
            "FROM templates AS tmp " \
            "LEFT JOIN master_templates AS mtmp " \
                "ON mtmp.template_id=tmp.template_id " \
            "WHERE mtmp.pulsar_id=%d " \
                "AND mtmp.obssystem_id=%d" % \
            (pulsar_id, obssystem_id)
    db.execute(query)
    if len(rows) > 1:
        raise errors.InconsistentDatabaseError("There are too many (%d) " \
                                            "master templates for pulsar #%d" % \
                                            (len(rows), pulsar_id ))
    elif len(rows) == 0:
        return None, None
    else:
        mastertmp_id, path, fn = rows[0]
        if path is None or fn is None:
            return None, None
        else:
            return mastertmp_id, os.path.join(path, fn)


def make_proc_diagnostics_dir(fn, proc_id):
    """Given an archive, create the appropriate diagnostics
        directory, and cross-references.

        Inputs:
            fn: The file to create a diagnostic directory for.
            proc_id: The processing ID number to create a diagnostic
                directory for.
        
        Outputs:
            dir: The diagnostic directory's name.
    """
    basedir = epu.get_archive_dir(fn, \
                    data_archive_location=config.diagnostics_location)
    dir = os.path.join(basedir, "procid_%d" % proc_id)
    # Make sure directory exists
    if not os.path.isdir(dir):
        # Create directory
        print_info("Making diagnostic directory: %s" % dir, 2)
        os.makedirs(dir, 0770)

    crossrefdir = os.path.join(config.diagnostics_location, "processing")
    if not os.path.isdir(crossrefdir):
        # Create directory
        print_info("Making diagnostic crossref dir: %s" % crossrefdir, 2)
        os.makedirs(crossrefdir, 0770)

    crossref = os.path.join(crossrefdir, "procid_%d" % proc_id)
    if not os.path.islink(crossref):
        # Create symlink
        print_info("Making crossref to diagnostic dir: %s" % crossref, 2)
        os.symlink(dir, crossref)


def execute(cmd, stdout=subprocess.PIPE, stderr=sys.stderr, \
                dir=None, stdinstr=None):
    """Execute the command 'cmd' after logging the command
        to STDOUT. Execute the command in the directory 'dir',
        which defaults to the current directory is not provided.

        Output standard output to 'stdout' and standard
        error to 'stderr'. Both are strings containing filenames.
        If values are None, the out/err streams are not recorded.
        By default stdout is subprocess.PIPE and stderr is sent 
        to sys.stderr.

        If stdinstr is not None, send the string to the command as
        data in the stdin stream.

        Returns (stdoutdata, stderrdata). These will both be None, 
        unless subprocess.PIPE is provided.
    """
    # Log command to stdout
    print_debug(cmd, "syscalls")

    stdoutfile = False
    stderrfile = False
    if type(stdout) == types.StringType:
        stdout = open(stdout, 'w')
        stdoutfile = True
    if type(stderr) == types.StringType:
        stderr = open(stderr, 'w')
        stderrfile = True

    if stdinstr is not None:
        print_debug("Sending the following to cmd's stdin: %s" % stdinstr, \
                        "syscalls")
        # Run (and time) the command. Check for errors.
        pipe = subprocess.Popen(cmd, shell=True, cwd=dir, \
                            stdin=subprocess.PIPE, 
                            stdout=stdout, stderr=stderr)
        (stdoutdata, stderrdata) = pipe.communicate(stdinstr)
    else:
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
                            action=self.SetAllDebugModes, \
                            help="Turn on all debugging modes. (Same as -d/--debug).")
        group.add_argument('--set-debug-mode', nargs=1, \
                            action=self.SetDebugMode, \
                            help="Turn on specified debugging mode. Use " \
                                "--list-debug-modes to see the list of " \
                                "available modes and descriptions. " \
                                "(Default: all debugging modes are off)")
        group.add_argument('--list-debug-modes', nargs=0, \
                            action=self.ListDebugModes, \
                            help="List available debugging modes and " \
                                "descriptions, then exit")
    class TurnUpVerbosity(argparse.Action):
        def __call__(self, parse, namespace, values, option_string):
            config.verbosity += 1

    class SetDebugMode(argparse.Action): 
        def __call__(self, parser, namespace, values, option_string):
            config.debug.set_mode_on(values[0])

    class SetAllDebugModes(argparse.Action): 
        def __call__(self, parser, namespace, values, option_string):
            config.debug.set_allmodes_on()

    class ListDebugModes(argparse.Action): 
        def __call__(self, parser, namespace, values, option_string):
            print "Available debugging modes:"
            for name, desc in config.debug.modes:
                print "    %s: %s" % (name, desc)
            sys.exit(1)

def get_file_from_id(ftype,type_id, existdb):
    """Return a file path for a particular file ID and 
        cross-check the md5sum with the database.
        
        Inputs:
            ftype: File type. Can be 'rawfile', 'parfile', or 'template'.
            id: The ID number from the database.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            fn: The full file path.
    """
    if ftype.lower() not in ("rawfile", "parfile", "template"):
        raise errors.UnrecognizedValueError("File type (%s) is not recognized." % \
                                                ftype)

    column_name = "%s_id" % ftype
    table_name = "%ss" % ftype

    print_info("Looking-up file of type %s with %s_id == %d" % \
                (ftype, column_name, id))

    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    
    query = "SELECT filename, " \
                "filepath, " \
                "md5sum " \
            "FROM %s " \
            "WHERE %s = %d" % \
                (table_name, column_name, id)
    db.execute(query)

    rows = db.fetchall()
    if len(rows) == 1:
        filename, filepath, md5sum_DB = rows[0]
    else:
        raise errors.DatabaseError("Bad number (%d) of matching " \
                                    "%s IDs" % (ftype, len(rows)))
        
    fullpath = os.path.join(filepath,filename)
    Verify_file_path(fullpath)
    md5sum_file = Get_md5sum(fullpath)
    if md5sum_DB != md5sum_file:
        raise errors.FileError("md5sum check failed! MD5 from DB (%s) " \
                                "!= MD5 from archived file (%s)" % \
                                (md5sum_DB, md5sum_file))
    
    if not existdb:
        # Close the DB connection we opened
        db.close()
    
    return fullpath


def DB_load_TOA(tempo2_toa_string, process_id, template_id, rawfile_id, existdb=None):
    """Upload a TOA to the database.

        Inputs:
            tempo2_toa_string: A TEMPO2 format toa line.
            process_id: The ID of the processing run that generated the TOA.
            template_id: The ID of the template used for generating the TOA.
            rawfile_id: The ID of the raw data file the TOA is derived from.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Outputs:
            None
    """
    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    
    toastr = tempo2_toa_string
    freq = float(toastr.split()[1])
    imjd = float(toastr.split()[2].split(".")[0])
    fmjd = float("0." + toastr.split()[2].split(".")[1])
    err = float(toastr.split()[3])

    # Writes values to the toa table
    query = "INSERT INTO toa " \
                "(process_id, " \
                "template_id, " \
                "rawfile_id, " \
                "pulsar_id, " \
                "obssystem_id, " \
                "imjd, " \
                "fmjd, " \
                "freq, " \
                "toa_unc_us) " \
            "SELECT %d, " % process_id + \
                "%d, " % template_id + \
                "r.rawfile_id, " + \
                "r.pulsar_id, " + \
                "r.obssystem_id, " + \
                "%.20g, " % imjd + \
                "%.20g, " % fmjd + \
                "%.20g, " % freq + \
                "%.20g, " % err + \
            "FROM rawfiles AS r " \
            "WHERE r.rawfile_id=%d" % rawfile_id
    db.execute(query)
    
    if not existdb:
        # Close the DB connection we opened
        db.close()
    
