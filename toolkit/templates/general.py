from toaster import database
from toaster import errors
from toaster import utils
from toaster.utils import notify

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
    
    notify.print_info("Getting template ID for %s using "
                    "filename and md5sum" % args.parfile, 2)
    path, fn = os.path.split(os.path.abspath(template))
    md5sum = utils.Get_md5sum(template)
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
    notify.print_info("Looking-up raw file with ID=%d" % template_id, 2)

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
    utils.Verify_file_path(fullpath)
    if verify_md5:
        notify.print_info("Confirming MD5 sum of %s matches what is " \
                    "stored in DB (%s)" % (fullpath, md5sum_DB), 2)
                    
        md5sum_file = utils.Get_md5sum(fullpath)
        if md5sum_DB != md5sum_file:
            raise errors.FileError("md5sum check of %s failed! MD5 from " \
                                "DB (%s) != MD5 from file (%s)" % \
                                (fullpath, md5sum_DB, md5sum_file))
    return fullpath


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


