"""Useful, general functions and data.
"""

import sys
import os
import shutil
import os.path
import datetime
import argparse
import hashlib
import subprocess
import types
import tempfile
import inspect
import warnings
import re
import pwd

import errors
import colour
import config
import database

##############################################################################
# GLOBAL DEFENITIONS
##############################################################################
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
# CACHES
##############################################################################
pulsarid_cache = {}
pulsarname_cache = {}


##############################################################################
# Functions
##############################################################################
def Verify_file_path(file):
    #Verify that file exists
    print_info("Verifying file: %s" % file, 2)
    if not os.path.isfile(file):
        raise errors.FileError("File %s does not exist, you dumb dummy!" % file)

    #Determine path (will retrieve absolute path)
    file_path, file_name = os.path.split(os.path.abspath(file))
    print_info("File %s exists!" % os.path.join(file_path, file_name), 3)
    return file_path, file_name


def Give_UTC_now():
    utcnow = datetime.datetime.utcnow()
    return utcnow.strftime("%b %d, %Y - %H:%M:%S (UTC)")


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
    db.connect()
    select = db.select([db.users.c.user_name, \
                        db.users.c.user_id])
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        # Close the DB connection we opened
        db.close()

    # Create the mapping
    userids = {}
    for uname, uid in rows:
        userids[uname] = uid
    return userids


def get_pulsarid_cache(existdb=None, update=False):
    """Return a dictionary mapping pulsar names to pulsar ids.

        Input:
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)
            update: If True, update the cache even if it already
                exists. (Default: Don't update)

        Output:
            pulsarid_cache: A dictionary with pulsar names as keys
                    and pulsar ids as values.
    """
    global pulsarid_cache
    if update or not pulsarid_cache:
        db = existdb or database.Database()
        db.connect()

        select = db.select([db.pulsar_aliases.c.pulsar_alias, \
                            db.pulsar_aliases.c.pulsar_id])
        result = db.execute(select)
        rows = result.fetchall()
        result.close()
        if not existdb:
            db.close()
        # Create the mapping
        for row in rows:
            pulsarid_cache[row['pulsar_alias']] = row['pulsar_id']
    return pulsarid_cache


def get_pulsarname_cache(existdb=None, update=False):
    """Return a dictionary mapping pulsar ids to pulsar info.

        Input:
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)
            update: If True, update the cache even if it already
                exists. (Default: Don't update)

        Output:
            pulsarinfo_cache: A dictionary with pulsar ids as keys
                    and pulsar names as values.
    """
    global pulsarname_cache
    if update or not pulsarname_cache:
        db = existdb or database.Database()
        db.connect()

        select = db.select([db.pulsars.c.pulsar_name, \
                            db.pulsars.c.pulsar_id])
        result = db.execute(select)
        rows = result.fetchall()
        result.close()
        if not existdb:
            db.close()
        # Create the mapping
        for row in rows:
            pulsarname_cache[row['pulsar_id']] = row['pulsar_name']
    return pulsarname_cache


def get_pulsarname(pulsar_id):
    """Return the preferred names for a pulsar given an ID.
        
        Inputs:
            pulsar_id: The ID number of the pulsar in the DB.

        Output:
            pulsar_name: The preferred name of the pulsar.
    """
    cache = get_pulsarname_cache()
    if pulsar_id not in cache:
        raise errors.UnrecognizedValueError("The pulsar ID (%d) does not " \
                                "appear in the pulsarname_cache!" % pulsar_id)
    return cache[pulsar_id]


def get_prefname(alias):
    """Given a pulsar alias return that pulsar's preferred name.

        Input:
            alias: The name/alias of the pulsar.
            
        Output:
            pulsar_name: The preferred name of the pulsar.
    """
    return get_pulsarname(get_pulsarid(alias))

def get_pulsarid(alias):
    """Given a pulsar name/alias return its pulsar_id number,
        or raise an error.

        Input:
            alias: The name/alias of the pulsar.

        Output:
            pulsar_id: The corresponding pulsar_id value.
    """
    cache = get_pulsarid_cache()
    if alias not in cache:
        raise errors.UnrecognizedValueError("The pulsar name/alias '%s' does " \
                                    "not appear in the pulsarid_cache!" % alias)
    return cache[alias]


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
    db.connect()

    select = db.select([db.telescopes.c.telescope_name, \
                        db.obssystems.c.frontend, \
                        db.obssystems.c.backend, \
                        db.obssystems.c.obssystem_id], \
                from_obj=[db.obssystems.\
                    outerjoin(db.telescopes, \
                        onclause=db.telescopes.c.telescope_id == \
                                db.obssystems.c.telescope_id)])
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        # Close the DB connection we opened
        db.close()

    # Create the mapping
    obssystemids = {}
    for row in rows:
        obssystemids[(row['telescope_name'].lower(), \
                      row['frontend'].lower(), \
                      row['backend'].lower())] = row['obssystem_id']
    return obssystemids


def get_telescope_info(alias, existdb=None):
    """Given a telescope alias return the info from the 
        matching telescope columns.

        Inputs:
            alias: The telescope's alias.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            row: The matching database row.
                NOTE: the columns in the return RowProxy object can
                be referenced like a dictionary, using column names.
    """
    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    db.connect()
    
    select = db.select([db.telescopes.c.telescope_id, \
                        db.telescopes.c.telescope_name, \
                        db.telescopes.c.telescope_abbrev, \
                        db.telescopes.c.telescope_code], \
                from_obj=[db.telescopes.\
                    join(db.telescope_aliases, \
                    onclause=db.telescopes.c.telescope_id == \
                            db.telescope_aliases.c.telescope_id)], \
                distinct=db.telescopes.c.telescope_id).\
                where(db.telescope_aliases.c.telescope_alias.like(alias))
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        # Close the DB connection we opened
        db.close()
    
    if len(rows) > 1:
        raise errors.BadInputError("Multiple matches (%d) for this " \
                                    "telescope alias (%s)! Be more " \
                                    "specific." % (len(rows), alias))
    elif len(rows) == 0:
        raise errors.BadInputError("Telescope alias provided (%s) doesn't " \
                                    "match any telescope entries in the " \
                                    "database." % alias)
    else:
        row = rows[0]
    return row


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
            psrname = get_prefname(params['name'])
    tinfo = get_telescope_info(site)
    sitedir = tinfo['telescope_abbrev']
    
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

        if int_re.match(valstr):
            # Looks like an int. Cast to int.
            val = int(valstr)
        elif float_re.match(valstr):
            # Looks like a float. Change 'D' to 'E' and cast to float.
            val = float(valstr.upper().replace('D','E'))
        else:
            # Doesn't seem like a number. Leave as string.
            val = valstr

        params[key.lower()] = val
    if "psrj" in params:
        params['pulsar_id'] = get_pulsarid(params['psrj'])
        params['name'] = params['psrj']
    elif "psrb" in params:
        params['pulsar_id'] = get_pulsarid(params['psrb'])
        params['name'] = params['psrb']
    else:
        params['pulsar_id'] = get_pulsarids(params['psr'])
        params['name'] = params['psr']
    
    # Translate a few parameters
    if params.has_key('binary'):
        params['binary_model'] = params['binary']
    if params.has_key('e'):
        params['ecc'] = params['e']

    # normalise pulsar name
    params['name'] = get_prefname(params['name'])
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
    	        "rcvr", "basis", "backend", "mjd"]
    params = get_header_vals(fn, hdritems)

    # Normalise telescope name
    tinfo = get_telescope_info(params['telescop'])
    params['telescop'] = tinfo['telescope_name']

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
    try:
        psr_id = get_pulsarid(params['name'])
    except errors.UnrecognizedValueError:
        raise errors.FileError("The pulsar name %s (from file %s) is not " \
                            "recognized." % (params['name'], fn))
    else:
        # Normalise pulsar name
        params['name'] = get_prefname(params['name'])
        params['pulsar_id'] = psr_id

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
    username = pwd.getpwuid(os.getuid())[0]
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
    db.connect()
    db.begin() # open a transaction

    # Check to see if this combination of versions is in the database
    select = db.select([db.versions.c.version_id]).\
                where((db.versions.c.pipeline_githash==pipeline_githash) & \
                      (db.versions.c.psrchive_githash==psrchive_githash) & \
                      (db.versions.c.tempo2_cvsrevno=='Not available'))
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if len(rows) > 1:
        db.rollback()
        if not existdb:
            # Close the DB connection we opened
            db.close()
        raise errors.DatabaseError("There are too many (%d) matching " \
                                    "version IDs" % len(rows))
    elif len(rows) == 1:
        version_id = rows[0].version_id
    else:
        # Insert the current versions
        ins = db.versions.insert()
        values = {'pipeline_githash':pipeline_githash, \
                  'psrchive_githash':psrchive_githash, \
                  'tempo2_cvsrevno':'Not available'}
        result = db.execute(ins, values)
        # Get the newly add version ID
        version_id = result.inserted_primary_key[0]
        result.close()
    
    db.commit()
    
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
    if is_gitrepo_dirty(config.epta_pipeline_dir):
        if config.debug.GITTEST:
            warnings.warn("Git repository is dirty! Will tolerate because " \
                            "pipeline debugging is on.", \
                            errors.EptaPipelineWarning)
        else:
            raise errors.EptaPipelineError("Pipeline's git repository is dirty. " \
                                            "Aborting!")

    if is_gitrepo_dirty(config.psrchive_dir):
        raise errors.EptaPipelineError("PSRCHIVE's git repository is dirty. " \
                                        "Clean up your act!")


def archive_file(toarchive, destdir):
    if not config.archive:
        # Configured to not archive files
        warnings.warn("Configurations are set to _not_ archive files. " \
                        "Doing nothing...", errors.EptaPipelineWarning)
        return toarchive
    srcdir, fn = os.path.split(toarchive)
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
        print_info("Moving %s to %s" % (toarchive, dest), 2)
        shutil.copy2(toarchive, dest)
        
        # Check that file copied successfully
        srcmd5 = Get_md5sum(toarchive)
        srcsize = os.path.getsize(toarchive)
        destmd5 = Get_md5sum(dest)
        destsize = os.path.getsize(dest)
        if (srcmd5==destmd5) and (srcsize==destsize):
            if config.move_on_archive:
                os.remove(toarchive)
                print_info("File (%s) successfully moved to %s." % \
                            (toarchive, dest), 2)
            else:
                print_info("File (%s) successfully copied to %s." % \
                            (toarchive, dest), 2)
        else:
            raise errors.ArchivingError("File copy failed! (Source MD5: %s, " \
                        "Dest MD5: %s; Source size: %d, Dest size: %d)" % \
                        (srcmd5, destmd5, srcsize, destmd5))
    elif os.path.abspath(destdir) == os.path.abspath(srcdir):
        # File is already located in its destination
        # Do nothing
        warnings.warn("Source file %s is already in the archive (and in " \
                        "the correct place). Doing nothing..." % toarchive, \
                        errors.EptaPipelineWarning)
        pass
    else:
        # Another file with the same name is the destination directory
        # Compare the files
        srcmd5 = Get_md5sum(toarchive)
        srcsize = os.path.getsize(toarchive)
        destmd5 = Get_md5sum(dest)
        destsize = os.path.getsize(dest)
        if (srcmd5==destmd5) and (srcsize==destsize):
            # Files are the same, so remove src as if we moved it
            # (taking credit for work that was already done...)
            warnings.warn("Another version of this file (%s), with " \
                            "the same size (%d bytes) and the same " \
                            "MD5 (%s) is already in the archive. " \
                            "Doing nothing..." % \
                            (toarchive, destsize, destmd5), \
                            errors.EptaPipelineWarning)
        else:
            # The files are not the same! This is not good.
            # Raise an exception.
            raise errors.ArchivingError("File (%s) cannot be archived. " \
                    "There is already a file archived by that name " \
                    "in the appropriate archive location (%s), but " \
                    "the two files are _not_ identical. " \
                    "(source: MD5=%s, size=%d bytes; dest: MD5=%s, " \
                    "size=%d bytes)" % \
                    (toarchive, dest, srcmd5, srcsize, destmd5, destsize))

    # Change permissions so the file can no longer be written to
    print_info("Changing permissions of archived file to 440", 2)
    os.chmod(dest, 0440) # "0440" is an integer in base 8. It works
                         # the same way 440 does for chmod on cmdline

    print_info("%s archived to %s (%s)" % (toarchive, dest, Give_UTC_now()), 1)

    return dest


def hash_password(pw):
    return hashlib.md5(pw).hexdigest()


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
    db.connect()
    select = db.select([db.master_parfiles.c.parfile_id, \
                        db.parfiles.c.filepath, \
                        db.parfiles.c.filename], \
                (db.master_parfiles.c.parfile_id==db.parfiles.c.parfile_id) & \
                (db.master_parfiles.c.pulsar_id==pulsar_id))
    result = db.execute(select)
    rows = db.fetchall()
    result.close()
    db.close()
    if len(rows) > 1:
        raise errors.InconsistentDatabaseError("There are too many (%d) " \
                                            "master parfiles for pulsar #%d" % \
                                            (len(rows), pulsar_id ))
    elif len(rows) == 0:
        return None, None
    else:
        row = rows[0]
        if row['filepath'] is None or row['filename'] is None:
            return None, None
        else:
            return row['parfile_id'], \
                    os.path.join(row['filepath'], row['filename'])


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
    db.connect()

    select = db.select([db.templates.c.template_id, \
                        db.templates.c.filename, \
                        db.templates.c.filepath]).\
                where((db.master_templates.c.template_id == \
                            db.templates.c.template_id) & \
                      (db.master_templates.c.pulsar_id == pulsar_id) & \
                      (db.master_templates.c.obssystem_id == obssystem_id))
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if len(rows) > 1:
        raise errors.InconsistentDatabaseError("There are too many (%d) " \
                                            "master templates for pulsar #%d" % \
                                            (len(rows), pulsar_id ))
    elif len(rows) == 0:
        return None, None
    else:
        mastertmp_id = rows[0]['template_id']
        path = rows[0]['filepath']
        fn = rows[0]['filename']
        if path is None or fn is None:
            return None, None
        else:
            return mastertmp_id, os.path.join(path, fn)


def create_rawfile_diagnostic_plots(archivefn, dir, suffix=""):
    """Given an archive create diagnostic plots to be uploaded
        to the DB.

        Inputs:
            archivefn: The archive's name.
            dir: The directory where the plots should be created.
            suffix: A string to add to the end of the base output
                file name. (Default: Don't add a suffix).
            
        NOTE: No dot, underscore, etc is added between the base
            file name and the suffix.

        Outputs:
            diagfns: A dictionary of diagnostic files created.
                The keys are the plot type descriptions, and 
                the values are the full path to the plots.
    """
    hdr = get_header_vals(archivefn, ['name', 'intmjd', 'fracmjd', 'npol'])
    hdr['secs'] = int(hdr['fracmjd']*24*3600+0.5) # Add 0.5 so result is 
                                                  # rounded to nearest int
    basefn = "%(name)s_%(intmjd)05d_%(secs)05d" % hdr
    # Add the suffix to the end of the base file name
    basefn += suffix
    # To keep track of all diagnostics created, keyed by their description
    diagfns = {}

    # Create a temporary file
    tmpfile, tmpfn = tempfile.mkstemp(prefix='toaster_tmp', \
                        suffix='_diag.png', dir=config.base_tmp_dir)
    os.close(tmpfile)

    # Create Time vs. Phase plot (psrplot -p time).
    outfn = os.path.join(dir, "%s.time.png" % basefn)
    execute("psrplot -p time -j DFp -c 'above:c=%s' -D %s/PNG %s" % \
                    (os.path.split(archivefn)[-1], tmpfn, archivefn))
    shutil.move(tmpfn, outfn)
    diagfns['Time vs. Phase'] = outfn

    # Create Freq vs. Phase plot (pav -dGTp).
    outfn = os.path.join(dir, "%s.freq.png" % basefn)
    execute("psrplot -p freq -j DTp -c 'above:c=%s' -D %s/PNG %s" % \
                    (os.path.split(archivefn)[-1], tmpfn, archivefn))
    shutil.move(tmpfn, outfn)
    diagfns['Freq vs. Phase'] = outfn

    if hdr['npol'] > 1:
        # Create summed profile, with polarisation information.
        outfn = os.path.join(dir, "%s.polprof.png" % basefn)
        execute("psrplot -p stokes -j DFT -c 'above:c=%s' -D %s/PNG %s" % \
                    (os.path.split(archivefn)[-1], tmpfn, archivefn))
        shutil.move(tmpfn, outfn)
        diagfns['Pol. Profile'] = outfn
    else:
        # Create plain summed profile.
        outfn = os.path.join(dir, "%s.prof.png" % basefn)
        execute("psrplot -p flux -j DFTp -c 'above:c=%s' -D %s/PNG %s" % \
                    (os.path.split(archivefn)[-1], tmpfn, archivefn))
        shutil.move(tmpfn, outfn)
        diagfns['Profile'] = outfn
    
    return diagfns


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
    basedir = get_archive_dir(fn, \
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
    
    return dir

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
    print_debug(cmd, "syscalls", stepsback=2)

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


def print_debug(msg, category, stepsback=1):
    """Print a debugging message if the given debugging category
        is turned on.

        The message will be colourized as 'debug'.

        Inputs:
            msg: The message to print.
            category: The debugging category of the message.
            stepsback: The number of steps back into the call stack
                to get function calling information from. 
                (Default: 1).

        Outputs:
            None
    """
    if config.debug.is_on(category):
        if config.helpful_debugging:
            # Get caller info
            fn, lineno, funcnm = inspect.stack()[stepsback][1:4]
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
                            action=self.TurnUpVerbosity, \
                            help="Be more verbose. (Default: " \
                                 "verbosity level = %d)." % config.verbosity)
        group.add_argument('-q', '--less-verbose', nargs=0, \
                            action=self.TurnDownVerbosity, \
                            help="Be less verbose. (Default: " \
                                 "verbosity level = %d)." % config.verbosity)
        group.add_argument('--set-verbosity', nargs=1, dest='level', \
                            action=self.SetVerbosity, type=int, \
                            help="Set verbosity level. (Default: " \
                                 "verbosity level = %d)." % config.verbosity)

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
        group.add_argument('--set-debug-mode', nargs=1, dest='mode', \
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

    class TurnDownVerbosity(argparse.Action):
        def __call__(self, parse, namespace, values, option_string):
            config.verbosity -= 1

    class SetVerbosity(argparse.Action):
        def __call__(self, parse, namespace, values, option_string):
            config.verbosity = values[0]

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


def get_parfile_from_id(parfile_id, existdb=None, verify_md5=True):
    """Return the path to the raw file that has the given ID number.
        Optionally double check the file's MD5 sum, to make sure
        nothing strange has happened.

        Inputs:
            parfile_id: The ID number of the raw file to get
                a path for.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)
            verify_md5: If True, double check the file's MD5 sum.
                (Default: Perform MD5 check.)

        Output:
            fn: The full file path.
    """
    print_info("Looking-up raw file with ID=%d" % parfile_id, 2)

    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    db.connect()
    
    select = db.select([db.parfiles.c.filename, \
                        db.parfiles.c.filepath, \
                        db.parfiles.c.md5sum]).\
                where(db.parfiles.c.parfile_id==parfile_id)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        # Close the DB connection we opened
        db.close()

    if len(rows) == 1:
        filename = rows[0]['filename']
        filepath = rows[0]['filepath']
        md5sum_DB = rows[0]['md5sum']
    else:
        raise errors.IncosistentDatabaseError("Bad number of files (%d) " \
                            "with parfile_id=%d" % (len(rows), parfile_id))
        
    fullpath = os.path.join(filepath,filename)
    # Make sure the file exists
    Verify_file_path(fullpath)
    if verify_md5:
        print_info("Confirming MD5 sum of %s matches what is " \
                    "stored in DB (%s)" % (fullpath, md5sum_DB), 2)
                    
        md5sum_file = Get_md5sum(fullpath)
        if md5sum_DB != md5sum_file:
            raise errors.FileError("md5sum check of %s failed! MD5 from " \
                                "DB (%s) != MD5 from file (%s)" % \
                                (fullpath, md5sum_DB, md5sum_file))
    return fullpath


def get_template_from_id(template_id, existdb=None, verify_md5=True):
    """Return the path to the raw file that has the given ID number.
        Optionally double check the file's MD5 sum, to make sure
        nothing strange has happened.

        Inputs:
            template_id: The ID number of the raw file to get
                a path for.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)
            verify_md5: If True, double check the file's MD5 sum.
                (Default: Perform MD5 check.)

        Output:
            fn: The full file path.
    """
    print_info("Looking-up raw file with ID=%d" % template_id, 2)

    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    db.connect()
    
    select = db.select([db.templates.c.filename, \
                        db.templates.c.filepath, \
                        db.templates.c.md5sum]).\
                where(db.templates.c.template_id==template_id)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        # Close the DB connection we opened
        db.close()

    if len(rows) == 1:
        filename = rows[0]['filename']
        filepath = rows[0]['filepath']
        md5sum_DB = rows[0]['md5sum']
    else:
        raise errors.IncosistentDatabaseError("Bad number of files (%d) " \
                            "with template_id=%d" % (len(rows), template_id))
        
    fullpath = os.path.join(filepath,filename)
    # Make sure the file exists
    Verify_file_path(fullpath)
    if verify_md5:
        print_info("Confirming MD5 sum of %s matches what is " \
                    "stored in DB (%s)" % (fullpath, md5sum_DB), 2)
                    
        md5sum_file = Get_md5sum(fullpath)
        if md5sum_DB != md5sum_file:
            raise errors.FileError("md5sum check of %s failed! MD5 from " \
                                "DB (%s) != MD5 from file (%s)" % \
                                (fullpath, md5sum_DB, md5sum_file))
    return fullpath


def get_rawfile_from_id(rawfile_id, existdb=None, verify_md5=True):
    """Return the path to the raw file that has the given ID number.
        Optionally double check the file's MD5 sum, to make sure
        nothing strange has happened.

        Inputs:
            rawfile_id: The ID number of the raw file to get
                a path for.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)
            verify_md5: If True, double check the file's MD5 sum.
                (Default: Perform MD5 check.)

        Output:
            fn: The full file path.
    """
    print_info("Looking-up raw file with ID=%d" % rawfile_id, 2)

    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    db.connect()
    
    select = db.select([db.rawfiles.c.filename, \
                        db.rawfiles.c.filepath, \
                        db.rawfiles.c.md5sum]).\
                where(db.rawfiles.c.rawfile_id==rawfile_id)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        # Close the DB connection we opened
        db.close()

    if len(rows) == 1:
        filename = rows[0]['filename']
        filepath = rows[0]['filepath']
        md5sum_DB = rows[0]['md5sum']
    else:
        raise errors.IncosistentDatabaseError("Bad number of files (%d) " \
                            "with rawfile_id=%d" % (len(rows), rawfile_id))
        
    fullpath = os.path.join(filepath,filename)
    # Make sure the file exists
    Verify_file_path(fullpath)
    if verify_md5:
        print_info("Confirming MD5 sum of %s matches what is " \
                    "stored in DB (%s)" % (fullpath, md5sum_DB), 2)
                    
        md5sum_file = Get_md5sum(fullpath)
        if md5sum_DB != md5sum_file:
            raise errors.FileError("md5sum check of %s failed! MD5 from " \
                                "DB (%s) != MD5 from file (%s)" % \
                                (fullpath, md5sum_DB, md5sum_file))
    return fullpath


def parse_pat_output(patout):
    """Parse the output from 'pat'.
        
        Input:
            patout: The stdout output of running 'pat'.

        Output:
            toainfo: A list of dictionaries, each with
                information for a TOA.
    """
    toainfo = []
    for toastr in patout.split("\n"):
        toastr = toastr.strip()
        if toastr and (toastr != "FORMAT 1") and \
                    (not toastr.startswith("Plotting")):
            toasplit = toastr.split()
            freq = float(toasplit[1])
            imjd = float(toasplit[2].split(".")[0])
            fmjd = float("0." + toasplit[2].split(".")[1])
            err = float(toasplit[3])
            if '-gof' in toasplit:
                # The goodness-of-fit is only calculated for the 'FDM'
                # fitting method. The GoF value returned for other 
                # methods is innaccurate.
                gofvalstr = toasplit[toasplit.index('-gof')+1]
                if config.toa_fitting_method=='FDM' and gofvalstr!='*error*':
                    gof = float(gofvalstr)
                else:
                    gof = None
            if ('-bw' in toasplit) and ('-nchan' in toasplit):
                nchan = int(toasplit[toasplit.index('-nchan')+1])
                bw = float(toasplit[toasplit.index('-bw')+1])
                bw_per_toa = bw/nchan
            else:
                bw_per_toa = None
            if ('-length' in toasplit) and ('-nsubint' in toasplit):
                nsubint = int(toasplit[toasplit.index('-nsubint')+1])
                length = float(toasplit[toasplit.index('-length')+1])
                length_per_toa = length/nsubint
            else:
                length_per_toa = None
            if ('-nbin' in toasplit):
                nbin = int(toasplit[toasplit.index('-nbin')+1])
            toainfo.append({'freq':freq, \
                            'imjd':imjd, \
                            'fmjd':fmjd, \
                            'toa_unc_us':err, \
                            'goodness_of_fit':gof, \
                            'bw':bw_per_toa, \
                            'length':length_per_toa, \
                            'nbin':nbin})
    return toainfo


def load_toas(toainfo, process_id, template_id, rawfile_id, existdb=None):
    """Upload a TOA to the database.

        Inputs:
            toainfo: A list of dictionaries, each with
                information for a TOA.
            process_id: The ID of the processing run that generated the TOA.
            template_id: The ID of the template used for generating the TOA.
            rawfile_id: The ID of the raw data file the TOA is derived from.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Outputs:
            None
    """
    if not toainfo:
        raise errors.BadInputError("No TOA info was provided!")

    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    db.connect()
    
    db.begin() # Open a transaction
    select = db.select([db.rawfiles.c.pulsar_id, \
                        db.rawfiles.c.obssystem_id]).\
                where(db.rawfiles.c.rawfile_id==rawfile_id)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    
    if len(rows) > 1:
        db.rollback()
        raise errors.InconsistentDatabaseError("Too many (%d) matches " \
                                "for rawfile_id=%d." % (len(rows), rawfile_id))
    elif len(rows) == 0:
        db.rollback()
        raise errors.BadInputError("Weird! rawfile_id=%d has no matches " \
                                "in DB. How is it being used to generate " \
                                "TOAs? Wrong value passed to function?" % \
                                rawfile_id)
    else:
        pulsar_id = rows[0]['pulsar_id']
        obssystem_id = rows[0]['obssystem_id']
        # Writes values to the toa table
        ins = db.toas.insert()
        idinfo = {'process_id':process_id, \
                  'template_id':template_id, \
                  'rawfile_id':rawfile_id, \
                  'pulsar_id':pulsar_id, \
                  'obssystem_id':obssystem_id}
        toa_ids = []
        for values in toainfo:
            values.update(idinfo)
            result = db.execute(ins, values)
            toa_ids.append(result.inserted_primary_key[0])
            result.close()
        db.commit()
        if len(toa_ids) > 1:
            print_info("Added (%d) TOAs to DB." % len(toa_ids), 2)
        else:
            print_info("Added TOA to DB.", 2)
    
    if not existdb:
        # Close the DB connection we opened
        db.close()
    
    return toa_ids
