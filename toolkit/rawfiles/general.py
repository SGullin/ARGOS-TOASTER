import warnings

from toaster import database
from toaster import errors
from toaster import utils
from toaster.utils import notify
from utils.datafile import get_md5sum


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
    notify.print_info("Looking-up raw file with ID=%d" % rawfile_id, 2)

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
    utils.Verify_file_path(fullpath)
    if verify_md5:
        notify.print_info("Confirming MD5 sum of %s matches what is " \
                    "stored in DB (%s)" % (fullpath, md5sum_DB), 2)
                    
        md5sum_file = get_md5sum(fullpath)
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



