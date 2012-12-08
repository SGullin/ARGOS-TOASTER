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


SHORTNAME = 'setmaster'
DESCRIPTION = "Set a parfile already uploaded into the " \
              "database to be a master."


def add_arguments(parser):
    parser.add_argument('-n', '--dry-run', dest='dry_run', \
                        action='store_true', \
                        help="Do not modify database.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--parfile-id', dest='parfile_id', type=int, \
                        help="parfile_id value of parfile to set as master.")
    group.add_argument('--parfile', dest='parfile', type=str, \
                        help="Parfile to set as master.")


def main():
    # Connect to the database
    db = database.Database()
    db.connect()

    try:
        if args.parfile is not None:
            # parfile filename provided. Get parfile_id
            parfile_id = utils.get_parfile_id(args.parfile, db)
        else:
            parfile_id = args.parfile_id
        utils.print_info("Parfile ID to set as master: %d" % parfile_id, 1)
        if not args.dry_run:
            utils.set_as_master_parfile(parfile_id, db)
    finally:
        # Close DB connection
        db.close()


if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
