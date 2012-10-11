#!/usr/bin/env python
"""A script to load information about data archives into the database.
"""

import os.path
import warnings

import epta_pipeline_utils as epu
import errors
import database


def populate_rawfiles_table(db, archivefn, params):
    # md5sum helper function in epu
    md5 = epu.Get_md5sum(archivefn)
    path, fn = os.path.split(os.path.abspath(archivefn))
   
    trans = db.begin()
    # Does this file exist already?
    select = db.select([db.rawfiles.c.rawfile_id, \
                        db.rawfiles.c.pulsar_id]).\
                where(db.rawfiles.c.md5sum==md5)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if len(rows) > 1:
        db.rollback()
        raise errors.InconsistentDatabaseError("There are %d rawfiles " \
                    "with MD5 (%s) in the database already" % (len(rows), md5))
    elif len(rows) == 1:
        rawfile_id = rows[0]['rawfile_id']
        psr_id = rows[0]['pulsar_id']
        if psr_id == params['pulsar_id']:
            warnings.warn("A rawfile with this MD5 (%s) already exists " \
                            "in the DB for this pulsar (ID: %d). " \
                            "The file will not be re-registed into the DB. " \
                            "Doing nothing..." % (md5, psr_id), \
                            errors.EptaPipelineWarning)
            db.commit()
            return rawfile_id
        else:
            db.rollback()
            raise errors.InconsistentDatabaseError("A rawfile with this " \
                            "MD5 (%s) already exists in the DB, but for " \
                            "a different pulsar (ID: %d)!" % (md5, psr_id))
    else:
        # Based on its MD5, this rawfile doesn't already 
        # exist in the DB. Insert it.

        # Insert the file
        ins = db.rawfiles.insert()
        values = {'md5sum':md5, \
                  'filename':fn, \
                  'filepath':path, \
                  'coord':'%s,%s' % (params['ra'],params['dec'])}
        values.update(params)
        result = db.execute(ins, values)
        rawfile_id = result.inserted_primary_key[0]
        result.close()

        # Create rawfile diagnostics
        diagfns = epu.create_datafile_diagnostic_plots(archivefn, path)
        # Load processing diagnostics
        for diagtype, diagpath in diagfns.iteritems():
            diagdir, diagfn = os.path.split(diagpath)
            ins = db.raw_diagnostic_plots.insert()
            values = {'rawfile_id':rawfile_id, \
                      'filename':diagfn, \
                      'filepath':diagdir, \
                      'plot_type':diagtype}
            result = db.execute(ins, values)
            result.close()
            epu.print_info("Inserted rawfile diagnostic plot (type: %s)." % \
                        diagtype, 2)
    db.commit()
    return rawfile_id


def main():
    fn = args.infile
    rawfile_id = load_rawfile(fn)
    print "%s has been loaded to the DB. rawfile_id: %d" % \
            (fn, rawfile_id)


def load_rawfile(fn):
    # Connect to the database
    db = database.Database()
    db.connect()

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
    finally:
        # Close DB connection
        db.close()
    return rawfile_id
    

if __name__=='__main__':
    parser = epu.DefaultArguments(description="Archive a single raw file, " \
                                        "and load its info into the database.")
    parser.add_argument("infile", type=str, \
                        help="File name of the raw file to upload.")
    args = parser.parse_args()
    main()

