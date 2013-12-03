#!/usr/bin/env python
"""
Script to promote a previously uploaded timfile to be a master.
"""

import os.path
import warnings

import config

import database
import errors
import utils


SHORTNAME = 'setmaster'
DESCRIPTION = "Set a timfile already uploaded into the " \
              "database to be a master."


def add_arguments(parser):
    parser.add_argument('-n', '--dry-run', dest='dry_run', \
                        action='store_true', \
                        help="Do not modify database.")
    parser.add_argument('--timfile-id', dest='timfile_id', type=int, \
                        help="timfile_id value of timfile to set as master.")


def set_as_master_timfile(timfile_id, existdb=None):
    """Set a timfile, specified by its ID number, as the 
        master timfile for its pulsar/observing system 
        combination.

        Inputs:
            timfile_id: The ID of the timfile to set as
                a master timfile.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Ouputs:
            None
    """
    # Use the exisitng DB connection, or open a new one if None was provided
    db = existdb or database.Database()
    db.connect()

    trans = db.begin()
    # Check if this pulsar already has a master timfile in the DB
    select = db.select([db.timfiles.c.pulsar_id, \
                        db.master_timfiles.c.timfile_id.label('mtimid')]).\
                where((db.master_timfiles.c.pulsar_id == \
                            db.timfiles.c.pulsar_id) & \
                      (db.timfiles.c.timfile_id == timfile_id))
    result = db.execute(select)
    row = result.fetchone()
    result.close()
    if row:
        if row['mtimid']==timfile_id:
            warnings.warn("Timfile (ID: %d) is already the master timfile " \
                            "for this pulsar (ID: %d). Doing nothing..." % \
                            (row['mtimid'], row['pulsar_id']), \
                            errors.ToasterWarning)
            trans.rollback()
            if not existdb:
                db.close()
            return
        else:
            # Update the existing entry
            query = db.master_timfiles.update().\
                        where(db.master_timfiles.c.pulsar_id==row['pulsar_id'])
            values = {'timfile_id':timfile_id}
    else:
        # Insert a new entry
        query = db.master_timfiles.insert()
        select = db.select([db.timfiles.c.pulsar_id]).\
                    where(db.timfiles.c.timfile_id==timfile_id)
        result = db.execute(select)
        row = result.fetchone()
        result.close()
        
        values = {'timfile_id':timfile_id, \
                  'pulsar_id':row['pulsar_id']}
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


def main(args):
    if args.timfile_id is None:
        raise errors.BadInputError("A timfile_id must be provided!")

    # Connect to the database
    db = database.Database()
    db.connect()

    try:
        utils.print_info("Timfile ID to set as master: %d" % args.timfile_id, 1)
        if not args.dry_run:
            set_as_master_timfile(args.timfile_id, db)
    finally:
        # Close DB connection
        db.close()


if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
