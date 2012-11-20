#!/usr/bin/env python
"""
Script to promote a previously uploaded parfile to be a master.
"""

import os.path
import warnings

import config

import database
import errors
import utils


def set_as_master_parfile(db, parfile_id):
    db.begin()
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
            db.commit()
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
        db.rollback()
        raise
    else:
        db.commit()
        result.close()


def get_parfile_id(db, parfile):
    """Given a parfile path find its parfile_id number.
        
        Inputs:
            db: connected database.Database object.
            parfile: the path to a parfile.

        Output:
            parfile_id: the corresponding parfile_id value.
    """
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
    if len(rows) == 1:
        row = rows[0]
        if row['md5sum']==md5sum and row['filename']==fn:
            warnings.warn("A parfile with this filename and md5sum " \
                            "already exists in the DB.", \
                            errors.ToasterWarning)
            return row['parfile_id']
        elif row['md5sum']==md5sum:
            raise errors.FileError("A parfile (parfile_id=%d) with " \
                            "this md5sum, but a different filename " \
                            "exists in the DB." % row['parfile_id'])
        else:
            raise errors.FileError("A parfile (parfile_id=%d) with " \
                            "this filename, but a different md5sum " \
                            "exists in the DB." % row['parfile_id'])
    elif len(rows) == 0:
        raise errors.ToasterError("Input parfile (%s) does not appear " \
                                        "to be registered in the DB! " \
                                        "Use 'load_parfile.py' to load " \
                                        "it into the DB." % parfile)
    else:
        raise errors.InconsistentDatabaseError("Multiple parfiles have " \
                                                "the same file name!")
        
    
def main():
    # Connect to the database
    db = database.Database()
    db.connect()

    try:
        if args.parfile is not None:
            # parfile filename provided. Get parfile_id
            parfile_id = get_parfile_id(db, args.parfile)
        else:
            parfile_id = args.parfile_id
        utils.print_info("Parfile ID to set as master: %d" % parfile_id, 1)
        if not args.dry_run:
            set_as_master_parfile(db, parfile_id)
    finally:
        # Close DB connection
        db.close()


if __name__=='__main__':
    parser = utils.DefaultArguments(description="Set a parfile " \
                                              "already uploaded into the " \
                                              "database to be a master.")
    parser.add_argument('-n', '--dry-run', dest='dry_run', \
                        action='store_true', \
                        help="Do not modify database.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--parfile-id', dest='parfile_id', type=int, \
                        help="parfile_id value of parfile to set as master.")
    group.add_argument('--parfile', dest='parfile', type=str, \
                        help="Parfile to set as master.")
    args = parser.parse_args()
    main()
