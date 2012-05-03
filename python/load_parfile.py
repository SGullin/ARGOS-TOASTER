#!/usr/bin/env python2.6

"""
Script to upload par files to the EPTA timing database.
"""

# Import modules
import os.path
import warnings
import types

import database
import config
import errors
import epta_pipeline_utils as epu


def populate_parfiles_table(db, fn, params):
    # md5sum helper function in epu
    md5 = epu.Get_md5sum(fn);
    path, fn = os.path.split(os.path.abspath(fn))
    
    # Does this file exist already?
    query = "SELECT parfile_id, pulsar_id " \
            "FROM parfiles " \
            "WHERE md5sum = '%s'" % md5
    db.execute(query)
    rows = db.fetchall()
    if len(rows) > 1:
        raise errors.InconsistentDatabaseError("There are %d parfiles " \
                    "with MD5 (%s) in the database already" % (len(rows), md5))
    elif len(rows) == 1:
        parfile_id, psr_id = rows[0]
        if psr_id == params['pulsar_id']:
            warnings.warn("A parfile with this MD5 (%s) already exists " \
                            "in the DB for this pulsar (ID: %d). " \
                            "The file will not be re-registed into the DB. " \
                            "Doing nothing..." % (md5, psr_id), \
                            errors.EptaPipelineWarning)
        else:
            raise errors.InconsistentDatabaseError("A parfile with this " \
                            "MD5 (%s) already exists in the DB, but for " \
                            "a different pulsar (ID: %d)!" % (md5, psr_id))
    else:
        # Based on its MD5, this parfile doesn't already 
        # exist in the DB. Insert it.

        # Get column names for parfiles table
        query = "DESCRIBE parfiles"
        db.execute(query)
        rows = db.fetchall()
        colnames = [r[0].lower() for r in rows]
 
        # Insert the file
        query = "INSERT INTO parfiles " + \
                    "SET md5sum = '%s', " % md5 + \
                       "filename = '%s', " % fn + \
                       "filepath = '%s', " % path + \
                       "add_time = NOW() "
        for col in colnames:
            if col in params:
                if type(params[col]) is types.FloatType:
                    query += ", %s = %.25g" % (col, params[col])
                elif type(params[col]) is types.IntType:
                    query += ", %s = %d" % (col, params[col])
                else:
                    query += ", %s = '%s'" % (col, params[col])
        db.execute(query)
        
        # Get the template_id of the file that was just entered
        query = "SELECT LAST_INSERT_ID()"
        parfile_id = db.execute_and_fetchone(query)[0]
    return parfile_id 


def set_as_master_parfile(db, parfile_id, pulsar_id):
    query = "UPDATE pulsars " \
            "SET master_parfile_id = %d " \
            "WHERE pulsar_id = %s" %(parfile_id, pulsar_id)
    db.execute(query)


def main():
    fn = args.parfile
    parfile_id = load_parfile(fn)
    print "%s has been archived and loaded to the DB. rawfile_id: %d" % \
            (fn, parfile_id)


def load_parfile(fn):
    # Connect to the database
    db = database.Database()
    
    try:
        # Now load the parfile file into database
        epu.print_info("Working on %s (%s)" % (fn, epu.Give_UTC_now()), 1)
        
        # Check the parfile and parse it
        params = epu.prep_parfile(fn)

        # Move the parfile
        destdir = os.path.join(config.data_archive_location, \
                    'parfiles', params['name'])
        newfn = epu.archive_file(fn, destdir)

        epu.print_info("%s moved to %s (%s)" % (fn, newfn, epu.Give_UTC_now()), 1)

        # Register the parfile into the database
        parfile_id = populate_parfiles_table(db, newfn, params)
       
        masterpar_id, parfn = epu.get_master_parfile(params['pulsar_id'])
        if masterpar_id is None:
            # If this is the only parfile for this pulsar 
            # make sure it will be set as the master
            args.is_master = True

        if args.is_master:
            epu.print_info("Setting %s as master parfile (%s)" % \
                            (newfn, epu.Give_UTC_now()), 1)
            set_as_master_parfile(db, parfile_id, params['pulsar_id'])
        epu.print_info("Finished with %s - parfile_id=%d (%s)" % \
                        (fn, parfile_id, epu.Give_UTC_now()), 1)
    finally:
        # Close DB connection
        db.close()
    return parfile_id


if __name__=='__main__':
    parser = epu.DefaultArguments(description="Upoad a parfile into " \
                                                 "the database.")
    parser.add_argument('--master', dest='is_master', \
                         action='store_true', default=False, \
                         help="Whether or not the provided file is to be " \
                                "set as the master parfile.")
    #parser.add_argument( '--comments', dest='comments', required=True,
    #                     type = str,
    #                     help='Provide comments describing the par files.')
    parser.add_argument('parfile', type=str, \
                         help="Parameter file to upload.")
    args = parser.parse_args()
    main()
