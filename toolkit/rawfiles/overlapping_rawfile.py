#!/usr/bin/env python
"""
A script to determine if previously uploaded raw data archives overlap
in time with another observation of the same pulsar made with the same
observing system.

Patrick Lazarus, Feb. 22, 2013
"""

import sys
import os.path

import config
import database
import utils

SHORTNAME = 'overlaps'
DESCRIPTION = "Determine if any previously uploaded raw data archives " \
                "overlap in time with another observation of the same " \
                "pulsar made with the same observing system."


def add_arguments(parser):
    parser.add_argument("rawfile", nargs='?', type=str, \
                        help="File name of the raw archive compare with " \
                            "database.")


def find_overlaps(rawfile, existdb=None):
    """Given the name of a raw data file find rawfile entries 
        in the database of the same pulsar/observing system
        that overlap in time.

        Inputs:
            rawfile: The name of the raw file to compare database
                entries with.
            existdb: An (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            matches: A list of rawfile_id numbers of overlapping files.
    """
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    # Get info about the input raw file
    params = utils.prep_file(rawfile)

    mjdstart = params['mjd']
    mjdend = mjdstart + params['length']/86400.0

    select = db.select([db.rawfiles.c.rawfile_id, \
                        db.rawfiles.c.filepath, \
                        db.rawfiles.c.filename]).\
                where((db.rawfiles.c.pulsar_id==params['pulsar_id']) & \
                    (db.rawfiles.c.obssystem_id==params['obssystem_id']) & \
                    ((mjdstart < (db.rawfiles.c.mjd+(db.rawfiles.c.length/86400.0))) & \
                     (mjdend > db.rawfiles.c.mjd)))
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        db.close()

    return rows


def main(args):
    matches = find_overlaps(args.rawfile)
    utils.print_info("Number of overlapping files in DB: %d" % \
                        len(matches), 1)
    lines = ["Overlapping files:"]
    if matches:
        for match in matches:
            fn = os.path.join(match['filepath'], match['filename'])
            lines.append("    %s" % fn)
    else:
        lines.append("    None")    
    msg = "\n".join(lines)
    utils.print_info(msg, 2)
    sys.exit(len(matches))


if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
