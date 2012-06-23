#!/usr/bin/env python2.6
"""
Script to promote a previously uploaded parfile to be a master.
"""

# Imported modules
import os.path

import config
import database
import errors
import epta_pipeline_utils as epu


def set_as_mater_parfile(db, parfile_id):
    query = "REPLACE INTO pulsars " \
            "SET master_parfile_id=%d " % parfile_id
    db.execute(query)

def get_parfile_id(db. parfile):
    """Given a parfile path find its parfile_id number.
        
        Inputs:
            db: connected database.Database object.
            parfile: the path to a parfile.

        Output:
            parfile_id: the corresponding parfile_id value.
    """
    path, fn = os.path.split(os.path.abspath(parfile))
    md5sum = epu.Get_md5sum(parfile)
    query = "SELECT parfile_id, md5sum " \
            "FROM parfiles " \
            "WHERE filename=%s"
    db.execute(query, (fn,))
    rows = db.fetchall()
    if rows == 1:
        if rows[0].md5sum != md5sum:
            raise errors.FileError("Parfile (%s) found in database but " \
                                    "MD5sum in DB (%s) doesn't match MD5sum" \
                                    "of the file provided (%s)!" % \
                                    (filename, rows[0].md5sum, md5sum))
        else:
            return rows[0].parfile_id
    elif rows == 0:
        raise errors.EptaPipelineError("No matching parfile found! " \
                                        "Use 'load_parfile.py' to add " \
                                        "a new parfile to the DB.")
    else:
        raise errors.InconsistentDatabaseError("Multiple parfiles have " \
                                                "the same file name!")
        
    
def main():
    # Connect to the database
    db = database.Database()
    
    try:
        if args.parfile is not None:
            epu.print_info("Getting parfile ID for %s using filename" % \
                            args.parfile, 1)
            # parfile filename provided. Get parfile_id
            parfile_id = get_parfile_id(db, args.parfile)
        else:
            parfile_id = args.parfile_id
        epu.print_info("Parfile ID to set as master: %d" % parfile_id, 1)
        if not args.dry_run:
            set_as_master_parfile(db, parfile_id)
    finally:
        # Close DB connection
        db.close()


if __name__=='__main__':
    parser = epu.DefaultArguments(description="Set a parfile " \
                                              "already uploaded into the " \
                                              "database to be a master.")
    parser.add_argument('-n', '--dry-run', dest='dry_run', \
                        action='store_true', \
                        help="Do not modify database.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--parfile-id', dest='parfile_id', type=int, \
                        help="parfile_id value of parfile to set as master.")
    group.add_argument('--parfile', dest='parfile', type=int, \
                        help="Parfile to set as master.")
    args = parser.parse_args()
    main()
