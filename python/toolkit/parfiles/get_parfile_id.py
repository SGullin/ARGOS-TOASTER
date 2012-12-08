#!/usr/bin/env python

"""
This TOASTER utility script provides the user with a listing
of parfile_id values from the database to help the user choose which
input is most appropriate.

Patrick Lazarus, Dec. 9, 2011.
"""

import os.path
import datetime

import utils
import database
import errors
import colour


SHORTNAME = 'query'
DESCRIPTION = "Get a listing of parfile_id " \
              "values from the DB to help the user" \
              "find the appropriate one to use."


def add_arguments(parser):
    parser.add_argument('-p', '--psr', dest='pulsar_name', \
                        type=str, default='%', \
                        help="The pulsar to grab parfiles for. " \
                            "NOTE: SQL regular expression syntax may be used")
    parser.add_argument('--parfile-id', dest='parfile_id', \
                        type=int, default=None, \
                        help="A parfile ID. This is useful for checking " \
                            "the details of a single parfile, identified " \
                            "by its ID number. NOTE: No other parfiles " \
                            "will match if this option is provided.")
    parser.add_argument('-s', '--start-date', dest='start_date', \
                        type=str, default=None, \
                        help="Do not return parfiles added to the DB " \
                            "before this date. (Format: YYYY-MM-DD)")
    parser.add_argument('-e', '--end-date', dest='end_date', \
                        type=str, default=None, \
                        help="Do not return parfiles added to the DB " \
                            "after this date. (Format: YYYY-MM-DD)")


def main(args):
    parfiles = get_parfiles(args.pulsar_name, args.start_date, args.end_date, \
                            args.parfile_id)
    show_parfiles(parfiles)


def get_parfiles(psr, start=None, end=None, parid=None):
    """Return a dictionary of information for each parfile
        in the DB that matches the search criteria provided.

        Inputs:
            psr: A SQL-style regular expression to match with
                pulsar J- and B-names.
            start: string representation of a datetime object in
                a format understood by sql. no parfiles added
                before this date are returned.
            end: string representation of a datetime object in
                a format understood by sql. no parfiles added
                after this date are returned.
            parid: get the parfile with this parfile_id number
                only.

        Output:
            rows: A list of dicts for each matching row. 
                The following columns are included:
                    parfile_id, add_time, filename, filepath, 
                    PSRJ, and PSRB
    """
    db = database.Database()
    db.connect()

    whereclause = db.pulsar_aliases.c.pulsar_alias.like(psr)
    if parid is not None:
        whereclause &= (db.parfiles.c.parfile_id==parid)
    if start is not None:
        whereclause &= (db.parfiles.c.add_time >= start)
    if end is not None:
        whereclause &= (db.parfiles.c.add_time <= end)

    select = db.select([db.parfiles.c.parfile_id, \
                        db.parfiles.c.add_time, \
                        db.parfiles.c.filename, \
                        db.parfiles.c.filepath, \
                        db.parfiles.c.pulsar_id, \
                        db.pulsars.c.pulsar_name, \
                        db.master_parfiles.c.parfile_id.label('mparid')], \
                from_obj=[db.parfiles.\
                    join(db.pulsar_aliases, \
                        onclause=db.parfiles.c.pulsar_id == \
                                db.pulsar_aliases.c.pulsar_id).\
                    outerjoin(db.pulsars, \
                        onclause=db.parfiles.c.pulsar_id == \
                                db.pulsars.c.pulsar_id).\
                    outerjoin(db.master_parfiles, \
                        onclause=db.master_parfiles.c.parfile_id == \
                                    db.parfiles.c.parfile_id)], \
                distinct=db.parfiles.c.parfile_id).\
                where(whereclause)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    db.close()
    return rows


def show_parfiles(parfiles):
    if len(parfiles):
        for parfile in parfiles:
            print "- "*25
            print colour.cstring("Parfile ID:", underline=True, bold=True) + \
                    colour.cstring(" %d" % parfile['parfile_id'], bold=True)
            fn = os.path.join(parfile['filepath'], parfile['filename'])
            print "\nParfile: %s" % fn
            print "Pulsar name: %s" % parfile['pulsar_name']
            print "Master parfile? %s" % \
                        (((parfile['mparid'] is not None) and "Yes") or "No")
            print "Date and time parfile was added: %s" % \
                        parfile['add_time'].isoformat(' ')
            msg = "Parfile contents:\n\n"
            for line in open(fn, 'r'):
                msg += "%s\n" % line.strip()
            utils.print_info(msg, 1)
            print " -"*25
    else:
        raise errors.ToasterError("No parfiles match parameters provided!")


if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
