import pwd
import os
import types

from toaster import config
from toaster import database
from toaster import errors
from toaster.utils import notify

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
        user_name = pwd.getpwuid(os.getuid())[0]
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

        select = db.select([db.pulsar_aliases.c.pulsar_alias,
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
    if alias in cache:
        pulsar_id = cache[alias]
    else:
        raise errors.UnrecognizedValueError("The pulsar name/alias "
                                            "'%s' does not appear in "
                                            "the pulsarid_cache!" %
                                            alias)
    return pulsar_id


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
    if type(obssys_key) is not types.StringType:
        # Tuple of telescope, frontend, backend provided
        # Cast all strings to lowercase
        obssys_key = tuple([xx.lower() for xx in obssys_key])
    cache = get_obssystemid_cache()
    if obssys_key not in cache:
        raise errors.UnrecognizedValueError("The observing system (%s) " \
                                "does not appear in the obssysid_cache!" % \
                                str(obssys_key))
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

