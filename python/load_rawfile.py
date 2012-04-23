#!/usr/bin/python2.6
####################
# load_rawfile.py #
VERSION = 0.1
####################

#Imported modules
import sys
import os
import os.path
import glob
import shutil
import warnings

# import pipeline utilities
import epta_pipeline_utils as epu
import config
import errors
import database


def populate_rawfiles_table(db, fn, params):
    # md5sum helper function in epu
    md5 = epu.Get_md5sum(fn);
    path, fn = os.path.split(os.path.abspath(fn))
    
    # Does this file exist already?
    query = "SELECT rawfile_id, pulsar_id " \
            "FROM rawfiles " \
            "WHERE md5sum = '%s'" % md5
    db.execute(query)
    rows = db.fetchall()
    if len(rows) > 1:
        raise errors.InconsistentDatabaseError("There are %d rawfiles " \
                    "with MD5 (%s) in the database already" % (len(rows), md5))
    elif len(rows) == 1:
        rawfile_id, psr_id = rows[0]
        if psr_id == params['pulsar_id']:
            warnings.warn("A rawfile with this MD5 (%s) already exists " \
                            "in the DB for this pulsar (ID: %d). " \
                            "Doing nothing..." % (md5, psr_id), \
                            errors.EptaPipelineWarning)
        else:
            raise errors.InconsistentDatabaseError("A rawfile with this " \
                            "MD5 (%s) already exists in the DB, but for " \
                            "a different pulsar (ID: %d)!" % (md5, psr_id))
    else:
        # Based on its MD5, this rawfile doesn't already 
        # exist in the DB. Insert it.

        # Insert the file
        query = "INSERT INTO rawfiles " + \
                "SET md5sum = '%s', " % md5 + \
                    "filename = '%s', " % fn + \
                    "filepath = '%s', " % path + \
                    "user_id = '%s', " % params['user_id'] + \
                    "add_time = NOW(), " + \
                    "pulsar_id = '%s', " % params['pulsar_id'] + \
                    "obssystem_id = '%s', " % params['obssystem_id'] + \
                    "nbin = %d, " % int(params['nbin']) + \
                    "nchan = %d, " % int(params['nchan']) + \
                    "npol = %d, " % int(params['npol']) + \
                    "nsub = %d, " % int(params['nsub']) + \
                    "type = '%s', " % params['type'] + \
                    "site = '%s', " % params['telescop'] + \
                    "name = '%s', " % params['name'] + \
                    "coord = '%s,%s', " % (params['ra'],params['dec']) + \
                    "freq = %.15g, " % float(params['freq']) + \
                    "bw = %.15g, " % float(params['bw']) + \
                    "dm = %.15g, " % float(params['dm']) + \
                    "rm = %.15g, " % float(params['rm']) + \
                    "dmc = %.15g, " % float(params['dmc']) + \
                    "rmc = %.15g, " % float(params['rm_c']) + \
                    "polc = %.15g, " % float(params['pol_c']) + \
                    "scale = '%s', " % params['scale'] + \
                    "state = '%s', " % params['state'] + \
                    "length = %.15g, " % float(params['length']) + \
                    "rcvr_name = '%s', " % params['rcvr'] + \
                    "rcvr_basis = '%s', " % params['basis'] + \
                    "be_name = '%s'" % params['backend'] 
        db.execute(query)
        
        # Get the rawfile_id of the file that was just entered
        query = "SELECT LAST_INSERT_ID()"
        db.execute(query)
        rawfile_id = db.fetchone()[0]
    return rawfile_id


def main():
    fn = args.infile
    # Connect to the database
    db = database.Database()

    try:
        # Enter information in rawfiles table
        epu.print_info("Working on %s (%s)" % (fn, epu.Give_UTC_now()), 1)
        # Check the file and parse the header
        params = epu.prep_file(fn)
        
        # Move the File
        destdir = epu.get_archive_dir(fn, site=params['telescop'], \
                    backend=params['backend'], \
                    receiver=params['rcvr'], \
                    psrname=params['name'])
        newfn = epu.archive_file(fn, destdir)
        
        epu.print_info("%s moved to %s (%s)" % (fn, newfn, epu.Give_UTC_now()), 1)

        # Register the file into the database
        rawfile_id = populate_rawfiles_table(db, newfn, params)
        
        epu.print_info("Finished with %s - rawfile_id=%d (%s)" % \
                (fn, rawfile_id, epu.Give_UTC_now()), 1)

        # TODO: Create diagnostic plots and load them into the DB

        print "%s has been archived and loaded to the DB. rawfile_id: %d" % \
                (fn, rawfile_id)
    finally:
        # Close DB connection
        db.close()


if __name__=='__main__':
    parser = epu.DefaultArguments(description="Archive a single raw file, " \
                                        "and load its info into the database.")
    parser.add_argument("infile", type=str, \
                        help="File name of the raw file to upload.")
    args = parser.parse_args()
    main()

