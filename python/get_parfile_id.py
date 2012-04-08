#!/usr/bin/env python2.6

"""
This EPTA Pipeline utility script provides the user with a listing
of parfile_id values from the database to help the user choose which
input is most appropriate.

Patrick Lazarus, Dec. 9, 2011.
"""

import argparse
import os.path
import datetime

import epta_pipeline_utils as epu
import database
import errors
import colour

def main():
    parfiles = get_parfiles(args.pulsar_name, args.start_date, args.end_date)
    show_parfiles(parfiles)


def get_parfiles(psr, start=None, end=None):
    """Return a dictionary of information for each parfile
        in the DB that matches the search criteria provided.

        Inputs:
            psr: A SQL-style regular expression to match with
                pulsar J- and B-names.
            start: string represenation of a datetime object in
                a format understood by sql. no parfiles added
                before this date are returned.
            end: string represenation of a datetime object in
                a format understood by sql. no parfiles added
                after this date are returned.

        Output:
            rows: A list of dicts for each matching row. 
                The following columns are included:
                    parfile_id, add_time, filename, filepath, 
                    PSRJ, and PSRB
    """
    query = "SELECT par.parfile_id, " \
                   "par.add_time, " \
                   "par.filename, " \
                   "par.filepath, " \
                   "par.PSRJ, " \
                   "par.PSRB, " \
                   "IFNULL(p.master_parfile_id, 0) AS is_master " \
            "FROM parfiles AS par " \
            "LEFT JOIN pulsars AS p " \
                "ON p.master_parfile_id=par.parfile_id " \
            "WHERE (par.PSRJ LIKE %s OR par.PSRB LIKE %s) "
    query_args = [psr, psr]

    if start is not None:
        query += "AND add_time >= %s "
        query_args.append(start)
    if end is not None:
        query += "AND add_time <= %s "
        query_args.append(end)

    db = database.Database(cursor_class='dict')
    db.execute(query, query_args)
    parfiles = db.fetchall()
    db.close()
    return parfiles


def show_parfiles(parfiles):
    if len(parfiles):
        for pardict in parfiles:
            print "- "*25
            print colour.cstring("Parfile ID:", underline=True, bold=True) + \
                    colour.cstring(" %d" % pardict['parfile_id'], bold=True)
            fn = os.path.join(pardict['filepath'], pardict['filename'])
            print "\nParfile: %s" % fn
            print "Pulsar J-name: %s" % pardict['PSRJ']
            print "Pulsar B-name: %s" % pardict['PSRB']
            print "Master parfile? %s" % (pardict['is_master'] and "Yes" or "No")
            print "Date and time parfile was added: %s" % pardict['add_time'].isoformat(' ')
            msg = "Parfile contents:\n\n"
            for line in open(fn, 'r'):
                msg += "%s\n" % line.strip()
            epu.print_info(msg, 1)
            print " -"*25
    else:
        raise errors.EptaPipelineError("No parfiles match parameters provided!")


if __name__=='__main__':
    parser = epu.DefaultArguments(description="Get a listing of parfile_id " \
                                        "values from the DB to help the user" \
                                        "find the appropriate one to use.")
    parser.add_argument('-p', '--psr', dest='pulsar_name', \
                        type=str, default='%', \
                        help="The pulsar to grab parfiles for. " \
                            "NOTE: SQL regular expression syntax may be used")
    parser.add_argument('-s', '--start-date', dest='start_date', \
                        type=str, default=None, \
                        help="Do not return parfiles added to the DB " \
                            "before this date.")
    parser.add_argument('-e', '--end-date', dest='end_date', \
                        type=str, default=None, \
                        help="Do not return parfiles added to the DB " \
                            "after this date.")
    args = parser.parse_args()
    main()
