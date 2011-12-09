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

import epta_pipeline_utils
import MySQLdb


def main():
    parfiles = get_parfiles(args.pulsar_name, args.start_date, args.end_date)
    show_parfiles(parfiles, verbose=args.verbose)


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

    query = "SELECT parfile_id, " \
                   "add_time, " \
                   "filename, " \
                   "filepath, " \
                   "PSRJ, " \
                   "PSRB " \
            "FROM parfiles " \
            "WHERE (PSRJ LIKE %s OR PSRB LIKE %s) "
    query_args = [psr, psr]

    if start is not None:
        query += "AND add_time >= %s "
        query_args.append(start)
    if end is not None:
        query += "AND add_time <= %s "
        query_args.append(end)

    cursor, conn = epta_pipeline_utils.DBconnect(cursor_class=MySQLdb.cursors.DictCursor)
    cursor.execute(query, query_args)
    parfiles = cursor.fetchall()
    conn.close()
    return parfiles


def show_parfiles(parfiles, verbose=False):
    if len(parfiles):
        for pardict in parfiles:
            print "- "*25
            print "Parfile ID: %d" % pardict['parfile_id']
            fn = os.path.join(pardict['filepath'], pardict['filename'])
            print "    Parfile: %s" % fn
            print "    Pulsar J-name: %s; Pulsar B-name: %s" % \
                    (pardict['PSRJ'], pardict['PSRB'])
            print "    Date and time parfile was added: %s" % pardict['add_time'].isoformat(' ')
            if verbose:
                print "    Parfile contents:"
                for line in open(fn, 'r'):
                    print "        %s" % line.strip()
            print " -"*25
    else:
        print "*** NO MATCHING PARFILES! ***"


if __name__=='__main__':
    parser = argparse.ArgumentParser(description="Get a listing of parfile_id " \
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
    parser.add_argument('-v', '--verbose', dest='verbose', \
                        default=False, action='store_true', \
                        help="Be verbose; show the contents of the parfiles " \
                            "matching the search criteria provided. " \
                            "(Default: Don't be verbose.)")
    args = parser.parse_args()
    main()
