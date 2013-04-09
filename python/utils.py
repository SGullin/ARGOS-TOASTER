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

import config
import errors
import colour

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
pulsaralias_cache = {}
userid_cache = {}
userinfo_cache = {}
obssysid_cache = {}
obssysinfo_cache = {}
telescopeinfo_cache = {}

##############################################################################
# Functions
##############################################################################
def is_admin(user_id, existdb=None):
    """Return whether user has administrator privileges or not.

        Input:
            user_id: The ID of the user to check privileges for.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            admin: True, if the user has admin privileges. False, otherwise.
    """
    # Connect to the DB if necessary
    db = existdb or database.Database()
    db.connect()
   
    select = db.select([db.users.c.admin]).\
                where((db.users.c.user_id == user_id) & \
                        db.users.c.active)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()

    if len(rows) == 1:
        admin = rows[0]['admin']
    elif len(rows) > 1:
        raise errors.InconsistentDatabaseError("Multiple rows (%d) with " \
                                "user_id=%d!" % (len(rows), user_id))
    else:
        raise errors.UnrecognizedValueError("User ID (%d) is not " \
                                "recognized!" % user_id)
    return admin


def is_curator(user_id, pulsar_id, existdb=None):
    """Return whether user has curator privileges for the given
        pulsar.

        Inputs:
            user_id: The ID of the user to check privileges for.
            pulsar_id: The ID of the pulsar in question.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            curator: True if the user has curator privileges. 
                False otherwise.
    """
    # Check if user_id and pulsar_id are valid
    # Exceptions will be raise if no matches are found
    get_userinfo(user_id)
    get_pulsarname(pulsar_id)

    # Connect to the DB if necessary
    db = existdb or database.Database()
    db.connect()
    select = db.select([db.curators.c.user_id], \
                from_obj=[db.curators.\
                    outerjoin(db.users, \
                        onclause=db.curators.c.user_id == \
                                    db.users.c.user_id)]).\
                where((db.curators.c.pulsar_id == pulsar_id) & \
                        db.curators.c.user_id.in_((user_id,None)) & \
                        db.users.c.active)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()

    curator = bool(rows)
    return curator


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


def get_userid_cache(existdb=None, update=False):
    """Return a dictionary mapping user names to user ids.

        Input:
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)
            update: If True, update the cache even if it already
                exists. (Default: Don't update)

        Output:
            userid_cache: A dictionary with user names as keys 
                    and user ids as values.
    """
    global userid_cache
    if update or not userid_cache or not config.cfg.use_caches:
        userid_cache = {}
        db = existdb or database.Database()
        db.connect()

        select = db.select([db.users.c.user_name, \
                            db.users.c.user_id])
        result = db.execute(select)
        rows = result.fetchall()
        result.close()
        if not existdb:
            db.close()
        # Create the mapping
        for row in rows:
            userid_cache[row['user_name']] = row['user_id']
    return userid_cache


def get_userid(user_name=None):
    """Given a user name. Return the corresponding user_id.
    
        Input:
            user_name: A user name. (Default: return the ID of
                the current user)

        Output:
            user_id: The corresponding user_id.
    """
    if user_name is None:
        user_name = get_current_username()
    cache = get_userid_cache()
    if user_name not in cache:
        raise errors.UnrecognizedValueError("The user name (%s) does not " \
                                "appear in the userid_cache!" % user_name)
    return cache[user_name]


def get_userinfo_cache(existdb=None, update=False):
    """Return a dictionary mapping user ids to user info.

        Input:
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)
            update: If True, update the cache even if it already
                exists. (Default: Don't update)

        Output:
            userinfo_cache: A dictionary with user ids as keys
                    and user-info dicts as values.
    """
    global userinfo_cache
    if update or not userinfo_cache or not config.cfg.use_caches:
        userinfo_cache = {}
        db = existdb or database.Database()
        db.connect()

        select = db.select([db.users])
        result = db.execute(select)
        rows = result.fetchall()
        result.close()
        if not existdb:
            db.close()
        # Create the mapping
        for row in rows:
            userinfo_cache[row['user_id']] = row
    return userinfo_cache


def get_userinfo(user_id=None):
    """Given a user_id value. Return the info as a dictionary-like object.
    
        Input:
            user_id: The user_id number from the DB. \
                    (Default: get info for the current user)

        Output:
            user_info: A dictionary-like object of user info.
    """
    cache = get_userinfo_cache()
    if user_id is None:
        user_id = get_userid()
    if user_id not in cache:
        raise errors.UnrecognizedValueError("The user ID (%d) does not " \
                                "appear in the userinfo_cache!" % user_id)
    return cache[user_id]


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
    if update or not pulsarid_cache or not config.cfg.use_caches:
        pulsarid_cache = {}
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


def get_pulsaralias_cache(existdb=None, update=False):
    """Return a dictionary mapping pulsar IDs to pulsar aliases.

        Input:
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)
            update: If True, update the cache even if it already
                exists. (Default: Don't update)

        Output:
            pulsaralias_cache: A dictionary with pulsar IDs as keys
                    and a list of pulsar aliases as values.
    """
    global pulsaralias_cache
    if update or not pulsaralias_cache or not config.cfg.use_caches:
        pulsaralias_cache = {}
        pulsarid_cache = get_pulsarid_cache(existdb, update)
        for alias, psrid in pulsarid_cache.iteritems():
            aliases = pulsaralias_cache.setdefault(psrid, [])
            aliases.append(alias)
    return pulsaralias_cache


def get_pulsaraliases(pulsar_id):
    """Return the aliases for a pulsar given an ID.
        
        Inputs:
            pulsar_id: The ID number of the pulsar in the DB.

        Output:
            pulsar_aliases: The aliases of the pulsar.
    """
    cache = get_pulsaralias_cache()
    if pulsar_id not in cache:
        raise errors.UnrecognizedValueError("The pulsar ID (%d) does not " \
                                "appear in the pulsaralias_cache!" % pulsar_id)
    return cache[pulsar_id]


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
    if update or not pulsarname_cache or not config.cfg.use_caches:
        pulsarname_cache = {}
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


def get_obssystemid_cache(existdb=None, update=False):
    """Return a dictionary mapping fronend/backend combinations
        to obs system ids.

        Input:
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            obssystemids: A dictionary with a \
            (telescope, frontend, backend) tuple as keys
                    and obs system ids as values.
    """
    global obssysid_cache
    if update or not obssysid_cache or not config.cfg.use_caches:
        obssysid_cache = {}
        # Use the exisitng DB connection, or open a new one if None was provided
        db = existdb or database.Database()
        db.connect()
 
        select = db.select([db.telescope_aliases.c.telescope_alias, \
                            db.obssystems.c.frontend, \
                            db.obssystems.c.backend, \
                            db.obssystems.c.name, \
                            db.obssystems.c.obssystem_id], \
                    from_obj=[db.telescope_aliases.\
                        outerjoin(db.telescopes, \
                            onclause=db.telescopes.c.telescope_id == \
                                    db.telescope_aliases.c.telescope_id).\
                        outerjoin(db.obssystems, \
                            onclause=db.telescopes.c.telescope_id == \
                                    db.obssystems.c.telescope_id)]).\
                    where(db.obssystems.c.obssystem_id != None)
        result = db.execute(select)
        rows = result.fetchall()
        result.close()
        if not existdb:
            # Close the DB connection we opened
            db.close()
 
        # Create the mapping
        for row in rows:
            obssysid_cache[(row['telescope_alias'].lower(), \
                          row['frontend'].lower(), \
                          row['backend'].lower())] = row['obssystem_id']
            obssysid_cache[row['name']] = row['obssystem_id']
    return obssysid_cache


def get_obssysid(obssys_key):
    """Given a telescope, frontend, backend return the
        corresponding observing system ID.

        Input:
            obssys_key: The observing system's name, or 
                telescope, frontend, backend combination.

        Output:
            obssys_id: The corresponding observing system's ID.
    """
    cache = get_obssystemid_cache()
    if obssys_key not in cache:
        raise errors.UnrecognizedValueError("The observing system (%s) " \
                                "does not appear in the obssysid_cache!" % \
                                obssys_key)
    return cache[obssys_key]


def get_obssysinfo_cache(existdb=None, update=False):
    """Return a dictionary mapping obssystem IDs to 
        observing system info.

        Inputs:
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)
            update: If True, update the cache even if it already
                exists. (Default: Don't update)

        Output:
            obssysinfo_cache: A dictionary with obssystem IDs as 
                keys and observation info as values.
    """
    global obssysinfo_cache
    if update or not obssysinfo_cache or not config.cfg.use_caches:
        obssysinfo_cache = {}
        db = existdb or database.Database()
        db.connect()

        select = db.select([db.obssystems])
        result = db.execute(select)
        rows = result.fetchall()
        result.close()
        if not existdb:
            db.close()
        # Create the mapping
        for row in rows:
            obssysinfo_cache[row['obssystem_id']] = row
    return obssysinfo_cache


def get_obssysinfo(obssys_id):
    """Given an obssystem ID return the observing system info
        as a dictionary-like object.

        Input:
            obssys_id: The observing system ID.

        Output:
            obssys_info: A dictionary-like object of the observing
                system's info.
    """
    cache = get_obssysinfo_cache()
    if obssys_id not in cache:
        raise errors.UnrecognizedValueError("The observing system ID (%d) " \
                            "does not appear in the obssysinfo_cache!" % \
                            obssys_id)
    return cache[obssys_id]


def get_telescopeinfo_cache(existdb=None, update=False):
    """Return a dictionary mapping telescope aliases to 
        telescope info.

        Inputs:
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)
            update: If True, update the cache even if it already
                exists. (Default: Don't update)

        Output:
            telinfo_cache: A dictionary with telescope aliases as 
                keys and telescope info as values.
    """
    global telescopeinfo_cache
    if update or not telescopeinfo_cache or not config.cfg.use_caches:
        telescopeinfo_cache = {}
        db = existdb or database.Database()
        db.connect()
    
        select = db.select([db.telescopes.c.telescope_id, \
                            db.telescopes.c.telescope_name, \
                            db.telescopes.c.telescope_abbrev, \
                            db.telescopes.c.telescope_code, \
                            db.telescope_aliases.c.telescope_alias], \
                    from_obj=[db.telescopes.\
                        join(db.telescope_aliases, \
                        onclause=db.telescopes.c.telescope_id == \
                                db.telescope_aliases.c.telescope_id)], \
                    distinct=db.telescopes.c.telescope_id)
        result = db.execute(select)
        rows = result.fetchall()
        result.close()
        if not existdb:
            db.close()
        # Create the mapping
        for row in rows:
            telinfo = dict(row)
            telescope_alias = telinfo.pop('telescope_alias').lower()
            telescope_id = telinfo['telescope_id']
            telescopeinfo_cache[telescope_alias] = telinfo
            if telescope_id not in telescopeinfo_cache:
                telescopeinfo_cache[telescope_id] = telinfo
    return telescopeinfo_cache


def get_telescope_info(alias):
    """Given a telescope alias return the info from the 
        matching telescope columns.

        Inputs:
            alias: The telescope's alias.

        Output:
            tel_info: A dictionary object of the telescope's info.
    """
    if hasattr(alias, 'lower'):
        alias = alias.lower() # cast strings to lower case
    cache = get_telescopeinfo_cache()
    if alias not in cache:
        raise errors.UnrecognizedValueError("The telescope alias (%s) " \
                            "does not appear in the telescopeinfo_cache!" % \
                            alias)
    return cache[alias]


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
    if not len(hdritems):
        raise ValueError("No 'hdritems' requested to get from file header!")
    hdrstr = ",".join(hdritems)
    if '=' in hdrstr:
        raise ValueError("'hdritems' passed to 'get_header_vals' " \
                         "should not perform and assignments!")
    cmd = "/bin/bash -l -c \"vap -n -c '%s' '%s'\"" % (hdrstr, fn)
    outstr, errstr = execute(cmd)
    outvals = outstr.split()[(0-len(hdritems)):] # First value is filename (we don't need it)
    if errstr:
        raise errors.SystemCallError("The command: %s\nprinted to stderr:\n%s" % \
                                (cmd, errstr))
    elif len(outvals) != len(hdritems):
        raise errors.SystemCallError("The command: %s\nreturn the wrong " \
                            "number of values. (Was expecting %d, got %d.)" % \
                            (cmd, len(hdritems), len(outvals)))
    params = HeaderParams(fn)
    for key, val in zip(hdritems, outvals):
        if val == "INVALID":
            raise errors.SystemCallError("The vap header key '%s' " \
                                            "is invalid!" % key)
        elif val == "*" or val == "UNDEF":
            warnings.warn("The vap header key '%s' is not " \
                            "defined in this file (%s)" % (key, fn), \
                            errors.ToasterWarning)
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
   

def get_archive_dir(fn, data_archive_location=None, params=None):
    """Given a file name return where it should be archived.

        Input:
            fn: The name of the file to archive.
            data_archive_location: The base directory of the 
                archive. (Default: use location listed in config
                file).
            params: A HeaderParams object containing header
                parameters of the data file. (Default: create
                a throw-away HeaderParams object and populate
                it as necessary). NOTE: A dictionary object
                containing the required params can also be used.

        Output:
            dir: The directory where the file should be archived.
    """
    if data_archive_location is None:
        data_archive_location = config.cfg.data_archive_location
    if params is None:
        params = get_header_vals(fn, [])
    
    subdir = config.cfg.data_archive_layout % params
    archivedir = os.path.join(data_archive_location, subdir)
    archivedir = os.path.abspath(archivedir)
    if not archivedir.startswith(os.path.abspath(data_archive_location)):
        raise errors.ArchivingError("Archive directory for '%s' (%s) is " \
                        "not inside the specified data archive location: %s. " \
                        "Please check the 'data_archive_layout' parameter in " \
                        "the config file." % \
                        (fn, archivedir, data_archive_location))
    return archivedir


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
        params['pulsar_id'] = get_pulsarid(params['psr'])
        params['name'] = params['psr']
    
    # Translate a few parameters
    if params.has_key('binary'):
        params['binary_model'] = params['binary']
    if params.has_key('e'):
        params['ecc'] = params['e']
    
    # Do some error checking
    if params.has_key('sini') and type(params['sini']) is types.StringType:
        # 'SINI' parameter can be 'KIN' in this case omit 'SINI' from
        # the database.
        params.pop('sini')

    # normalise pulsar name
    params['name'] = get_prefname(params['name'])
    params['user_id'] = get_userid()
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
                # "dmc", "rm_c", "pol_c", # The names of these header params 
                                          # vary with psrchive version
      	        "scale", "state", "length", \
    	        "rcvr", "basis", "backend", "mjd"]
    params = get_header_vals(fn, hdritems)

    # Normalise telescope name
    tinfo = get_telescope_info(params['telescop'])
    params['telescop'] = tinfo['telescope_name']
    params.update(tinfo)

    # Check if obssystem_id, pulsar_id, user_id can be found
    obssys_key = (params['telescop'].lower(), params['rcvr'].lower(), \
                                params['backend'].lower())
    obssys_ids = get_obssystemid_cache()
    if obssys_key not in obssys_ids:
        t, r, b = obssys_key
        raise errors.FileError("The observing system combination in the file " \
                            "%s is not registered in the database. " \
                            "(Telescope: %s, Receiver: %s; Backend: %s)." % \
                            (fn, t, r, b))
    else:
        params['obssystem_id'] = obssys_ids[obssys_key]
        obssysinfo = get_obssysinfo(params['obssystem_id'])
        params['band_descriptor'] = obssysinfo['band_descriptor']
        params['obssys_name'] = obssysinfo['name']
    
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

        params['user_id'] = get_userid()
    return params


def get_current_username():
    return pwd.getpwuid(os.getuid())[0]


def is_gitrepo(repodir):
    """Return True if the given dir is a git repository.

        Input:
            repodir: The location of the git repository.

        Output:
            is_git: True if directory is part of a git repository. False otherwise.
    """
    print_info("Checking if directory '%s' contains a Git repo..." % repodir, 2)
    try:
        stdout, stderr = execute("git rev-parse", dir=repodir, \
                                    stderr=open(os.devnull))
    except errors.SystemCallError:
        # Exit code is non-zero
        return False
    else:
        # Success error code (i.e. dir is in a git repo)
        return True


def is_gitrepo_dirty(repodir):
    """Return True if the git repository has local changes.

        Inputs:
            repodir: The location of the git repository.

        Output:
            is_dirty: True if git repository has local changes. False otherwise.
    """
    print_info("Checking if Git repo at '%s' is dirty..." % repodir, 2)
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
                        errors.ToasterWarning)
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
    pipeline_githash = get_githash(os.path.dirname(__file__))
    if is_gitrepo(config.cfg.psrchive_dir):
        psrchive_githash = get_githash(config.cfg.psrchive_dir)
    else:
        warnings.warn("PSRCHIVE directory (%s) is not a git repository! " \
                        "Falling back to 'psrchive --version' for version " \
                        "information." % config.cfg.psrchive_dir, \
                        errors.ToasterWarning)
        stdout, stderr = execute("psrchive --version")
        psrchive_githash = stdout.strip()
    
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
    if is_gitrepo_dirty(os.path.abspath(os.path.dirname(__file__))):
        if config.debug.GITTEST:
            warnings.warn("Git repository is dirty! Will tolerate because " \
                            "pipeline debugging is on.", \
                            errors.ToasterWarning)
        else:
            raise errors.ToasterError("Pipeline's git repository is dirty. " \
                                            "Aborting!")
    if not is_gitrepo(config.cfg.psrchive_dir):
        warnings.warn("PSRCHIVE directory (%s) is not a git repository!" % \
                        config.cfg.psrchive_dir, errors.ToasterWarning)
    elif is_gitrepo_dirty(config.cfg.psrchive_dir):
        raise errors.ToasterError("PSRCHIVE's git repository is dirty. " \
                                        "Clean up your act!")


def archive_file(toarchive, destdir):
    if not config.cfg.archive:
        # Configured to not archive files
        warnings.warn("Configurations are set to _not_ archive files. " \
                        "Doing nothing...", errors.ToasterWarning)
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
            if config.cfg.move_on_archive:
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
                        errors.ToasterWarning)
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
                            errors.ToasterWarning)
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
    db.close()
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
    diagnostics_location = os.path.join(config.cfg.data_archive_location, "diagnostics")
    params = prep_file(fn)
    basedir = get_archive_dir(fn, params=params, \
                    data_archive_location=diagnostics_location)
    dir = os.path.join(basedir, "procid_%d" % proc_id)
    # Make sure directory exists
    if not os.path.isdir(dir):
        # Create directory
        print_info("Making diagnostic directory: %s" % dir, 2)
        os.makedirs(dir, 0770)

    crossrefdir = os.path.join(diagnostics_location, "processing")
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
    if dir is not None:
        msg = "(In %s)\n%s" % (dir, cmd)
    else:
        msg = cmd
    print_debug(msg, "syscalls", stepsback=2)

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
        pipe = subprocess.Popen(cmd, shell=True, cwd=dir , \
                            stdout=stdout)#, stderr=stderr)
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


def print_success(msg):
    """Print a success message.

        The message is colourized with the preset 'success' mode.

        Inputs:
            msg: The message to print.

        Outputs:
            None
    """
    colour.cprint(msg, 'success')
    sys.stdout.flush()

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
    if config.cfg.verbosity >= level:
        if config.cfg.excessive_verbosity:
            # Get caller info
            fn, lineno, funcnm = inspect.stack()[1][1:4]
            colour.cprint("INFO (level: %d) [%s:%d - %s(...)]:" % 
                    (level, os.path.split(fn)[-1], lineno, funcnm), 'infohdr')
            msg = msg.replace('\n', '\n    ')
            colour.cprint("    %s" % msg, 'info')
        else:
            colour.cprint(msg, 'info')
        sys.stdout.flush()


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
        if config.cfg.helpful_debugging:
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
        raise errors.InconsistentDatabaseError("Bad number of files (%d) " \
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


def get_template_id(template, existdb=None):
    """Given a template file path find its template_id number.
        
        Inputs:
            template: the path to a template file.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            template_id: the corresponding template_id value.
    """
    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    db.connect()
    
    utils.print_info("Getting template ID for %s using "
                    "filename and md5sum" % args.parfile, 2)
    path, fn = os.path.split(os.path.abspath(template))
    md5sum = Get_md5sum(template)
    select = db.select([db.templates.c.template_id, \
                        db.templates.c.filename, \
                        db.templates.c.md5sum]).\
                where((db.template.c.md5sum==md5sum) | (
                        db.templates.c.filename==fn))
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        db.close()
    if len(rows) == 1:
        row = rows[0]
        if row['md5sum']==md5sum and row['filename']==fn:
            return row['template_id']
        elif row['md5sum']==md5sum:
            raise errors.FileError("A template (template_id=%d) with " \
                            "this md5sum, but a different filename " \
                            "exists in the DB." % row['template_id'])
        elif row['filename']==fn:
            raise errors.FileError("A template (template_id=%d) with " \
                            "this filename, but a different md5sum " \
                            "exists in the DB." % row['template_id'])
        else:
            raise errors.InconsistentDatabaseError("A template (template_id=%d) " \
                            "matches our query, but neither its md5sum (%s), " \
                            "nor its filename (%s) appears to match! " \
                            "This should never happen!" % 
                            (row['template_id'], row['md5sum'], row['fn']))
    elif len(rows) == 0:
        raise errors.ToasterError("Input template (%s) does not appear " \
                                        "to be registered in the DB! " \
                                        "Use 'load_template.py' to load " \
                                        "it into the DB." % template)
    else:
        raise errors.InconsistentDatabaseError("Multiple (%s) templates " \
                                    "match the given file name or md5sum!" % \
                                    len(rows))


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
        raise errors.InconsistentDatabaseError("Bad number of files (%d) " \
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


def set_as_master_parfile(parfile_id, existdb=None):
    """Set a parfile, specified by its ID number, as the 
        master parfile for its pulsar/observing system 
        combination.

        Inputs:
            parfile_id: The ID of the parfile to set as
                a master parfile.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Ouputs:
            None
    """
    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    db.connect()

    trans = db.begin()
    # Check if this pulsar already has a master parfile in the DB
    select = db.select([db.parfiles.c.pulsar_id, \
                        db.master_parfiles.c.parfile_id.label('mparid')]).\
                where((db.master_parfiles.c.pulsar_id == \
                            db.parfiles.c.pulsar_id) & \
                      (db.parfiles.c.parfile_id == parfile_id))
    result = db.execute(select)
    row = result.fetchone()
    result.close()
    if row:
        if row['mparid']==parfile_id:
            warnings.warn("Parfile (ID: %d) is already the master parfile " \
                            "for this pulsar (ID: %d). Doing nothing..." % \
                            (row['mparid'], row['pulsar_id']), \
                            errors.ToasterWarning)
            trans.commit()
            if not existdb:
                db.close()
            return
        else:
            # Update the existing entry
            query = db.master_parfiles.update().\
                        where(db.master_parfiles.c.pulsar_id==row['pulsar_id'])
            values = {'parfile_id':parfile_id}
    else:
        # Insert a new entry
        query = db.master_parfiles.insert()
        select = db.select([db.parfiles.c.pulsar_id]).\
                    where(db.parfiles.c.parfile_id==parfile_id)
        result = db.execute(select)
        row = result.fetchone()
        result.close()
        
        values = {'parfile_id':parfile_id, \
                  'pulsar_id':row['pulsar_id']}
    try:
        result = db.execute(query, values)
    except:
        trans.rollback()
        raise
    else:
        trans.commit()
        result.close()
    finally:
        if not existdb:
            db.close()


def get_parfile_id(parfile, existdb=None):
    """Given a parfile path find its parfile_id number.
        
        Inputs:
            parfile: the path to a parfile.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            parfile_id: the corresponding parfile_id value.
    """
    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    db.connect()
    
    utils.print_info("Getting parfile ID for %s using "
                    "filename and md5sum" % args.parfile, 2)
    path, fn = os.path.split(os.path.abspath(parfile))
    md5sum = utils.Get_md5sum(parfile)
    select = db.select([db.parfiles.c.md5sum, \
                        db.parfiles.c.filename, \
                        db.parfiles.c.parfile_id]).\
                where((db.parfiles.c.md5sum==md5sum) | \
                            (db.parfiles.c.filename==fn))
    
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        db.close()
    if len(rows) == 1:
        row = rows[0]
        if row['md5sum']==md5sum and row['filename']==fn:
            return row['parfile_id']
        elif row['md5sum']==md5sum:
            raise errors.FileError("A parfile (parfile_id=%d) with " \
                            "this md5sum, but a different filename " \
                            "exists in the DB." % row['parfile_id'])
        elif row['filename']==fn:
            raise errors.FileError("A parfile (parfile_id=%d) with " \
                            "this filename, but a different md5sum " \
                            "exists in the DB." % row['parfile_id'])
        else:
            raise errors.InconsistentDatabaseError("A parfile (parfile_id=%d) " \
                            "matches our query, but neither its md5sum (%s), " \
                            "nor its filename (%s) appears to match! " \
                            "This should never happen!" % 
                            (row['parfile_id'], row['md5sum'], row['fn']))
    elif len(rows) == 0:
        raise errors.ToasterError("Input parfile (%s) does not appear " \
                                        "to be registered in the DB! " \
                                        "Use 'load_parfile.py' to load " \
                                        "it into the DB." % parfile)
    else:
        raise errors.InconsistentDatabaseError("Multiple (%s) parfiles " \
                                    "match the given file name or md5sum!" % \
                                    len(rows))



def set_as_master_template(template_id, existdb=None):
    """Set a template, specified by its ID number, as the 
        master template for its pulsar/observing system 
        combination.

        Inputs:
            template_id: The ID of the template to set as
                a master template.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Ouputs:
            None
    """
    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    db.connect()
    
    trans = db.begin()
    # Check if this pulsar/obssystem combiation already has a
    # Master template in the DB
    select = db.select([db.master_templates.c.template_id.label('mtempid'), \
                        db.templates.c.pulsar_id, \
                        db.templates.c.obssystem_id]).\
                where((db.master_templates.c.obssystem_id == \
                                db.templates.c.obssystem_id) & \
                        (db.master_templates.c.pulsar_id == \
                                db.templates.c.pulsar_id) & \
                        (db.templates.c.template_id==template_id))
    result = db.execute(select)
    row = result.fetchone()
    result.close()
    if row:
        if row['mtempid'] == template_id:
            warnings.warn("Template (ID: %d) is already the master " \
                            "template for this pulsar (ID: %d), " \
                            "observing system (ID: %d) combination. " \
                            "Doing nothing..." % (row['mtempid'], \
                            row['pulsar_id'], row['obssystem_id']), \
                            errors.ToasterWarning)
            trans.commit()
            if not existdb:
                db.close()
            return
        else:
            # Update the existing entry
            query = db.master_templates.update().\
                        where((db.master_templates.c.pulsar_id == \
                                    row['pulsar_id']) & \
                              (db.master_templates.c.obssystem_id == \
                                    row['obssystem_id']))
            values = {'template_id':template_id}
    else:
        # Insert a new entry
        query = db.master_templates.insert()
        select = db.select([db.templates.c.pulsar_id, \
                            db.templates.c.obssystem_id]).\
                    where(db.templates.c.template_id==template_id)
        result = db.execute(select)
        row = result.fetchone()
        result.close()

        values = {'template_id':template_id, \
                  'pulsar_id':row['pulsar_id'], \
                  'obssystem_id':row['obssystem_id']}
    try:
        result = db.execute(query, values)
    except:
        trans.rollback()
        raise
    else:
        trans.commit()
        result.close()
    finally:
        if not existdb:
            db.close()


def get_rawfile_diagnostics(rawfile_id, existdb=None):
    """Given a rawfile ID number return information about the 
        diagnostics.

        Inputs:
            rawfile_id: The ID number of the raw file to get
                a path for.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            float_diagnostics: A list of floating-point valued diagnostic info.
            plot_diagnostics: A list of plot diagnostic info.
    """
    db = existdb or database.Database()
    db.connect()
    
    select = db.select([db.raw_diagnostics.c.type, \
                        db.raw_diagnostics.c.value]).\
                where(db.raw_diagnostics.c.rawfile_id == \
                            rawfile_id)
    result = db.execute(select)
    diags = result.fetchall()
    result.close()
    select = db.select([db.raw_diagnostic_plots.c.plot_type, \
                        db.raw_diagnostic_plots.c.filepath, \
                        db.raw_diagnostic_plots.c.filename]).\
                where(db.raw_diagnostic_plots.c.rawfile_id == \
                            rawfile_id)
    result = db.execute(select)
    diag_plots = result.fetchall()
    result.close()
    if not existdb:
        db.close()
    return diags, diag_plots


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
                        db.rawfiles.c.md5sum, \
                        db.replacement_rawfiles.c.replacement_rawfile_id], \
                from_obj=[db.rawfiles.\
                    outerjoin(db.replacement_rawfiles, \
                        onclause=db.rawfiles.c.rawfile_id == \
                                db.replacement_rawfiles.c.obsolete_rawfile_id)]).\
                where(db.rawfiles.c.rawfile_id==rawfile_id)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        # Close the DB connection we opened
        db.close()

    if len(rows) == 1:
        if rows[0]['replacement_rawfile_id'] is not None:
            warnings.warn("The rawfile (ID: %d) has been superseded by " \
                        "another data file (rawfile ID: %d)." % \
                        (rawfile_id, rows[0]['replacement_rawfile_id']), \
                        errors.ToasterWarning)
        filename = rows[0]['filename']
        filepath = rows[0]['filepath']
        md5sum_DB = rows[0]['md5sum']
    else:
        raise errors.InconsistentDatabaseError("Bad number of files (%d) " \
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


def get_rawfile_info(rawfile_id, existdb=None):
    """Get and return a dictionary of rawfile info for the
        given rawfile_id.

        Input:
            rawfile_id: The ID number of the rawfile entry to get info about.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            rawfile_info: A dictionary-like object of info.
    """
    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    db.connect()
    
    select = db.select([db.rawfiles.c.filename, \
                        db.rawfiles.c.filepath, \
                        db.rawfiles.c.md5sum, \
                        db.rawfiles.c.pulsar_id, \
                        db.rawfiles.c.obssystem_id]).\
                where(db.rawfiles.c.rawfile_id==rawfile_id)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        # Close the DB connection we opened
        db.close()

    if len(rows) != 1:
        raise errors.InconsistentDatabaseError("Bad number of rawfiles " \
                                "(%d) with ID=%d!" % (len(rows), rawfile_id))
    return rows[0]


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
                if config.cfg.toa_fitting_method=='FDM' and gofvalstr!='*error*':
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


def load_toas(toainfo, existdb=None):
    """Upload a TOA to the database.

        Inputs:
            toainfo: A list of dictionaries, each with
                information for a TOA.
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
    
    # Write values to the toa table
    ins = db.toas.insert()
    toa_ids = []
    for values in toainfo:
        if 'toa_id' in values:
            raise errors.BadTOAFormat("TOA has already been loaded? " \
                                    "TOA ID: %d" % values['toa_id']) 
        result = db.execute(ins, values)
        toa_id = result.inserted_primary_key[0]
        result.close()
        toa_ids.append(toa_id)
        values['toa_id'] = toa_id
    db.commit()
    if len(toa_ids) > 1:
        print_info("Added %d TOAs to DB." % len(toa_ids), 2)
    else:
        print_info("Added TOA to DB.", 2)
    
    if not existdb:
        # Close the DB connection we opened
        db.close()
    
    return toa_ids


def sort_by_keys(tosort, keys):
    """Sort a list of dictionaries, or database rows
        by the list of keys provided. Keys provided 
        later in the list take precedence over earlier
        ones. If a key ends in '_r' sorting by that key
        will happen in reverse.

        Inputs:
            tosort: The list to sort.
            keys: The keys to use for sorting.

        Outputs:
            None - sorting is done in-place.
    """
    print_info("Sorting by keys (%s)" % " then ".join(keys), 3)
    for sortkey in keys:
        if sortkey.endswith("_r"):
            sortkey = sortkey[:-2]
            rev = True
            print_info("Reverse sorting by %s..." % sortkey, 2)
        else:
            rev = False
            print_info("Sorting by %s..." % sortkey, 2)
        if type(tosort[0][sortkey]) is types.StringType:
            tosort.sort(key=lambda x: x[sortkey].lower(), reverse=rev)
        else:
            tosort.sort(key=lambda x: x[sortkey], reverse=rev)
    

def set_warning_mode(mode=None, reset=True):
    """Add a simple warning filter.
        
        Inputs:
            mode: The action to use for warnings.
                (Default: take value of 'warnmode' configuration.
            reset: Remove warning filters previously set.

        Outputs:
            None
    """
    if mode is None:
        mode = config.cfg.warnmode
    if reset:
        warnings.resetwarnings()
    warnings.simplefilter(mode)


class DefaultArguments(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        self.added_std_group = False
        self.added_debug_group = False
        argparse.ArgumentParser.__init__(self, *args, **kwargs)

    def parse_args(self, *args, **kwargs):
        if not self._subparsers:
            # Add default groups just before parsing so it is the last set of
            # options displayed in help text
            self.add_standard_group()
            self.add_debug_group()
        args = argparse.ArgumentParser.parse_args(self, *args, **kwargs)
        if not self._subparsers:
            set_warning_mode(args.warnmode)
        return args

    def parse_known_args(self, *args, **kwargs):
        if not self._subparsers:
            # Add default groups just before parsing so it is the last set of
            # options displayed in help text
            self.add_standard_group()
            self.add_debug_group()
        args, leftovers = argparse.ArgumentParser.parse_known_args(self, *args, **kwargs)
        if not self._subparsers:
            set_warning_mode(args.warnmode)
        return args, leftovers

    def add_standard_group(self):
        if self.added_std_group:
            # Already added standard group
            return
        group = self.add_argument_group("Standard Options", \
                    "The following options get used by various programs.")
        group.add_argument('-v', '--more-verbose', nargs=0, \
                            action=self.TurnUpVerbosity, \
                            help="Be more verbose. (Default: " \
                                 "verbosity level = %d)." % config.cfg.verbosity)
        group.add_argument('-q', '--less-verbose', nargs=0, \
                            action=self.TurnDownVerbosity, \
                            help="Be less verbose. (Default: " \
                                 "verbosity level = %d)." % config.cfg.verbosity)
        group.add_argument('--set-verbosity', nargs=1, dest='level', \
                            action=self.SetVerbosity, type=int, \
                            help="Set verbosity level. (Default: " \
                                 "verbosity level = %d)." % config.cfg.verbosity)
        group.add_argument('-W', '--warning-mode', dest='warnmode', type=str, \
                            help="Set a filter that applies to all warnings. " \
                                "The behaviour of the filter is determined " \
                                "by the action provided. 'error' turns " \
                                "warnings into errors, 'ignore' causes " \
                                "warnings to be not printed. 'always' " \
                                "ensures all warnings are printed. " \
                                "(Default: print the first occurence of " \
                                "each warning.)")
        group.add_argument('--config-file', dest='cfg_file', \
                            action=self.LoadConfigFile, type=str, \
                            help="Configuration file to load. (Default: " \
                                "no personalized configs are loaded.)")
        self.added_std_group = True

    def add_debug_group(self):
        if self.added_debug_group:
            # Debug group has already been added
            return
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
        self.added_debug_group = True

    class LoadConfigFile(argparse.Action):
        def __call__(self, parser, namespace, values, option_string):
            config.cfg.load_configs(values)

    class TurnUpVerbosity(argparse.Action):
        def __call__(self, parser, namespace, values, option_string):
            config.cfg.verbosity += 1

    class TurnDownVerbosity(argparse.Action):
        def __call__(self, parser, namespace, values, option_string):
            config.cfg.verbosity -= 1
 
    class SetVerbosity(argparse.Action):
        def __call__(self, parser, namespace, values, option_string):
            config.cfg.verbosity = values[0]

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
                if desc is None:
                    continue
                print "    %s: %s" % (name, desc)
            sys.exit(1)


null = lambda x: x
class HeaderParams(dict):
    def __init__(self, fn, *args, **kwargs):
        self.fn = fn
        super(HeaderParams, self).__init__(*args, **kwargs)

    def __getitem__(self, key):
        if (type(key) in (type('str'), type(u'str'))) and key.endswith("_L"):
            filterfunc = str.lower
            key = key[:-2]
        elif (type(key) in (type('str'), type(u'str'))) and key.endswith("_U"):
            filterfunc = str.upper
            key = key[:-2]
        else:
            filterfunc = null
        if self.has_key(key):
            val = self.get_value(key)
            if type(val) in (type('str'), type(u'str')):
                return filterfunc(val)
            else:
                return val
        else:
            matches = [k for k in self.keys() if k.startswith(key)]
            if len(matches) == 1:
                val = self.get_value(matches[0])
                if type(val) in (type('str'), type(u'str')):
                    return filterfunc(val)
                else:
                    return val
            elif len(matches) > 1:
                raise errors.UnrecognizedValueError("The header parameter " \
                                    "abbreviation '%s' is ambiguous. ('%s' " \
                                    "all match)" % \
                                    (key, "', '".join(matches)))
            else:
                val = self.get_value(key)
                if type(val) in (type('str'), type(u'str')):
                    return filterfunc(val)
                else:
                    return val

    def get_value(self, key):
        if key not in self:
            params = get_header_vals(self.fn, [key])
            self.update(params)
        return super(self.__class__, self).__getitem__(key)
