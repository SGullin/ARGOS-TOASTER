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
   
    db.begin() # Begin a transaction
    # Does this file exist already?
    select = db.select([db.parfiles.c.parfile_id, db.parfiles.c.pulsar_id], \
                        db.parfiles.c.md5sum==md5)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if len(rows) > 1:
        db.rollback()
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
            db.rollback()
            raise errors.InconsistentDatabaseError("A parfile with this " \
                            "MD5 (%s) already exists in the DB, but for " \
                            "a different pulsar (ID: %d)!" % (md5, psr_id))
    else:
        # Based on its MD5, this parfile doesn't already 
        # exist in the DB. Insert it.

        # Insert the parfile
        ins = db.parfiles.insert()
        values = {'md5sum':md5, \
                  'filename':fn, \
                  'filepath':path}

        values.update(params)
        result = db.execute(ins, values)
        parfile_id = result.inserted_primary_key[0]
        result.close()
    
    db.commit()
    return parfile_id 


def set_as_master_parfile(db, parfile_id, pulsar_id):
    db.begin()
    # Check if this pulsar already has a master parfile in the DB
    select = db.master_parfiles.select().\
                    where(db.master_parfiles.c.pulsar_id==pulsar_id)
    result = db.execute(select)
    row = result.fetchone()
    result.close()
    if row:
        # Update the existing entry
        query = db.master_parfiles.update().\
                    where(db.master_parfiles.c.pulsar_id==pulsar_id)
        values = {'parfile_id':parfile_id}
    else:
        # Insert a new entry
        query = db.master_parfiles.insert()
        values = {'parfile_id':parfile_id, \
                  'pulsar_id':pulsar_id}
    try:
        result = db.execute(query, values)
    except:
        db.rollback()
        raise
    else:
        db.commit()
        result.close()


def main():
    fn = args.parfile
    parfile_id = load_parfile(fn)
    print "%s has been loaded to the DB. parfile_id: %d" % \
            (fn, parfile_id)


def load_parfile(fn):
    # Connect to the database
    db = database.Database()
    db.connect()

    try:
        # Now load the parfile file into database
        epu.print_info("Working on %s (%s)" % (fn, epu.Give_UTC_now()), 1)
        
        # Check the parfile and parse it
        params = epu.prep_parfile(fn)

        # Archive the parfile
        destdir = os.path.join(config.data_archive_location, \
                    'parfiles', params['name'])
        newfn = epu.archive_file(fn, destdir)

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
