import os
import os.path
import warnings

from toaster import database
from toaster import errors
from toaster.utils import notify
from utils import int_re, float_re
from utils import datafile
from utils import cache


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
    select = db.select([db.master_parfiles.c.parfile_id,
                        db.parfiles.c.filepath,
                        db.parfiles.c.filename],
                       (db.master_parfiles.c.parfile_id == db.parfiles.c.parfile_id) &
                       (db.master_parfiles.c.pulsar_id == pulsar_id))
    result = db.execute(select)
    rows = db.fetchall()
    result.close()
    db.close()
    if len(rows) > 1:
        raise errors.InconsistentDatabaseError("There are too many (%d) "
                                               "master parfiles for pulsar #%d" %
                                               (len(rows), pulsar_id))
    elif len(rows) == 0:
        return None, None
    else:
        row = rows[0]
        if row['filepath'] is None or row['filename'] is None:
            return None, None
        else:
            return row['parfile_id'], \
                os.path.join(row['filepath'], row['filename'])


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
    notify.print_info("Looking-up raw file with ID=%d" % parfile_id, 2)

    # Use the existing DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    db.connect()

    select = db.select([db.parfiles.c.filename,
                        db.parfiles.c.filepath,
                        db.parfiles.c.md5sum]). \
        where(db.parfiles.c.parfile_id == parfile_id)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        # Close the DB connection we opened
        db.close()

    if len(rows) == 1:
        filename = rows[0]['filename']
        filepath = rows[0]['filepath']
        md5sum_from_db = rows[0]['md5sum']
    else:
        raise errors.InconsistentDatabaseError("Bad number of files (%d) "
                                               "with parfile_id=%d" %
                                               (len(rows), parfile_id))

    fullpath = os.path.join(filepath, filename)
    # Make sure the file exists
    datafile.verify_file_path(fullpath)
    if verify_md5:
        notify.print_info("Confirming MD5 sum of %s matches what is "
                          "stored in DB (%s)" % (fullpath, md5sum_from_db), 2)

        md5sum_file = datafile.get_md5sum(fullpath)
        if md5sum_from_db != md5sum_file:
            raise errors.FileError("md5sum check of %s failed! MD5 from "
                                   "DB (%s) != MD5 from file (%s)" %
                                   (fullpath, md5sum_from_db, md5sum_file))
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
    select = db.select([db.parfiles.c.pulsar_id,
                        db.master_parfiles.c.parfile_id.label('mparid')]). \
        where((db.master_parfiles.c.pulsar_id ==
               db.parfiles.c.pulsar_id) &
              (db.parfiles.c.parfile_id == parfile_id))
    result = db.execute(select)
    row = result.fetchone()
    result.close()
    if row:
        if row['mparid'] == parfile_id:
            warnings.warn("Parfile (ID: %d) is already the master parfile "
                          "for this pulsar (ID: %d). Doing nothing..." %
                          (row['mparid'], row['pulsar_id']),
                          errors.ToasterWarning)
            trans.commit()
            if not existdb:
                db.close()
            return
        else:
            # Update the existing entry
            query = db.master_parfiles.update(). \
                where(db.master_parfiles.c.pulsar_id == row['pulsar_id'])
            values = {'parfile_id': parfile_id}
    else:
        # Insert a new entry
        query = db.master_parfiles.insert()
        select = db.select([db.parfiles.c.pulsar_id]). \
            where(db.parfiles.c.parfile_id == parfile_id)
        result = db.execute(select)
        row = result.fetchone()
        result.close()

        values = {'parfile_id': parfile_id,
                  'pulsar_id': row['pulsar_id']}
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

    notify.print_info("Getting parfile ID for %s using "
                      "filename and md5sum" % parfile, 2)
    path, fn = os.path.split(os.path.abspath(parfile))
    md5sum = datafile.get_md5sum(parfile)
    select = db.select([db.parfiles.c.md5sum,
                        db.parfiles.c.filename,
                        db.parfiles.c.parfile_id]). \
        where((db.parfiles.c.md5sum == md5sum) |
              (db.parfiles.c.filename == fn))

    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        db.close()
    if len(rows) == 1:
        row = rows[0]
        if row['md5sum'] == md5sum and row['filename'] == fn:
            return row['parfile_id']
        elif row['md5sum'] == md5sum:
            raise errors.FileError("A parfile (parfile_id=%d) with "
                                   "this md5sum, but a different filename "
                                   "exists in the DB." % row['parfile_id'])
        elif row['filename'] == fn:
            raise errors.FileError("A parfile (parfile_id=%d) with "
                                   "this filename, but a different md5sum "
                                   "exists in the DB." % row['parfile_id'])
        else:
            raise errors.InconsistentDatabaseError("A parfile (parfile_id=%d) "
                                                   "matches our query, but neither its md5sum (%s), "
                                                   "nor its filename (%s) appears to match! "
                                                   "This should never happen!" %
                                                   (row['parfile_id'], row['md5sum'], row['fn']))
    elif len(rows) == 0:
        raise errors.ToasterError("Input parfile (%s) does not appear "
                                  "to be registered in the DB! "
                                  "Use 'load_parfile.py' to load "
                                  "it into the DB." % parfile)
    else:
        raise errors.InconsistentDatabaseError("Multiple (%s) parfiles "
                                               "match the given file name or md5sum!" %
                                               len(rows))


def prep_parfile(fn):
    """Prepare parfile for archiving/loading.

        Also, perform some checks on the parfile to make sure we
        won't run into problems later. Checks performed:
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
    datafile.verify_file_path(fn)

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
            val = float(valstr.upper().replace('D', 'E'))
        else:
            # Doesn't seem like a number. Leave as string.
            val = valstr

        params[key.lower()] = val
    if "psrj" in params:
        params['pulsar_id'] = cache.get_pulsarid(params['psrj'])
        params['name'] = params['psrj']
    elif "psrb" in params:
        params['pulsar_id'] = cache.get_pulsarid(params['psrb'])
        params['name'] = params['psrb']
    else:
        params['pulsar_id'] = cache.get_pulsarid(params['psr'])
        params['name'] = params['psr']

    # Translate a few parameters
    if 'binary' in params:
        params['binary_model'] = params['binary']
    if 'e' in params:
        params['ecc'] = params['e']

    # Do some error checking
    if 'sini' in params and isinstance(params['sini'], str):
        # 'SINI' parameter can be 'KIN' in this case omit 'SINI' from
        # the database.
        params.pop('sini')

    # normalise pulsar name
    params['name'] = cache.get_prefname(params['name'])
    params['user_id'] = cache.get_userid()
    return params