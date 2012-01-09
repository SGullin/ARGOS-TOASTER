#!/usr/bin/env python2.6

"""
This EPTA Pipeline utility script provides the user with a listing
of rawfile_id values from the database to help the user choose which
input is most appropriate.

Patrick Lazarus, Jan. 8, 2012.
"""

import argparse
import os.path
import datetime
import warnings

import epta_pipeline_utils
import MySQLdb


def main():
    rawfiles = get_rawfiles(args.pulsar_name, args.start_date, args.end_date)
    show_rawfiles(rawfiles, verbose=args.verbose)


def get_rawfiles(args):
    """Return a dictionary of information for each rawfile
        in the DB that matches the search criteria provided.

        Input:
            args: Arugments from argparser.

        Output:
            rows: A list of dicts for each matching row. 
        """

    query = "SELECT r.rawfile_id, " \
                   "r.add_time, " \
                   "r.filename, " \
                   "r.filepath, " \
                   "psr.pulsar_name, " \
                   "t.name AS telescope_name, " \
                   "obs.obssystem_id, " \
                   "obs.name AS obssys_name, " \
                   "obs.frontend, " \
                   "obs.backend, " \
                   "obs.clock " \
            "FROM rawfiles AS r " \
            "LEFT JOIN pulsars AS psr " \
                "ON psr.pulsar_id=r.pulsar_id " \
            "LEFT JOIN obssystems AS obs " \
                "ON obs.obssystem_id=r.obssystem_id " \
            "LEFT JOIN telescopes AS t " \
                "ON t.telescope_id=obs.telescope_id "
            "WHERE (psr.pulsar_name LIKE %s) "
    query_args = [args.pulsar_name]

    if args.start is not None:
        query += "AND r.add_time >= %s "
        query_args.append(args.start)
    if args.end is not None:
        query += "AND r.add_time <= %s "
        query_args.append(args.end)

    # TODO: Implement MJD selection criteria 
    # when MJDs are added to rawfiles table
    warnings.warn("MJD selectioin criteria are _not_ implemented.")

    if args.obssys_id:
        query += "AND (obs.obssystem_id = %s) "
        query_args.append(args.obssys_id)
    if args.obssystem_name:
        query += "AND (obs.name LIKE %s) "
        query_args.append(args.obssystem_name)
    if args.telescope:
        query += "AND (t.name LIKE %s) "
        query_args.append(args.telescope)
    if args.frontend:
        query += "AND (obs.frontend LIKE %s) "
        query_args.append(args.frontend)
    if args.backend:
        query += "AND (obs.backend LIKE %s) "
        query_args.append(args.backend)
    if args.clock:
        query += "AND (obs.clock LIKE %s) "
        query_args.append(args.clock)

    cursor, conn = epta_pipeline_utils.DBconnect(cursor_class=MySQLdb.cursors.DictCursor)
    cursor.execute(query, query_args)
    rawfiles = cursor.fetchall()
    conn.close()
    return rawfiles


def show_rawfiles(rawfiles, verbose=False):
    if len(rawfiles):
        for rawdict in rawfiles:
            print "- "*25
            print "Rawfile ID: %d" % rawdict['rawfile_id']
            fn = os.path.join(rawdict['filepath'], rawdict['filename'])
            print "    Rawfile: %s" % fn
            print "    Pulsar name: %s" % rawdict['pulsar_name']
            print "    Date and time rawfile was added: %s" % rawdict['add_time'].isoformat(' ')
            if verbose:
                print "    Observing System ID: %d" % rawdict['obssystem_id']
                print "    Observing System Name: %s" % rawdict['obssys_name']
                print "    Telescope: %s" % rawdict['telescope_name']
                print "    Frontend: %s" % rawdict['frontend']
                print "    Backend: %s" % rawdict['backend']
                print "    Clock: %s" % rawdict['clock']
            print " -"*25
    else:
        print "*** NO MATCHING RAWFILES! ***"

if __name__=='__main__':
    parser = argparse.ArgumentParser(description="Get a listing of rawfile_id " \
                                        "values from the DB to help the user" \
                                        "find the appropriate one to use.")
    parser.add_argument('-p', '--psr', dest='pulsar_name', \
                        type=str, default='%', \
                        help="The pulsar to grab rawfiles for. " \
                            "NOTE: SQL regular expression syntax may be used")
    parser.add_argument('-s', '--start-date', dest='start_date', \
                        type=str, default=None, \
                        help="Do not return rawfiles added to the DB " \
                            "before this date.")
    parser.add_argument('-e', '--end-date', dest='end_date', \
                        type=str, default=None, \
                        help="Do not return rawfiles added to the DB " \
                            "after this date.")
    parser.add_argument('--start-mjd', dest='start_mjd', \
                        type=float, default=None, \
                        help="Do not return rawfiles from observations " \
                            "before this MJD.")
    parser.add_argument('--end-mjd', dest='end_mjd', \
                        type=float, default=None, \
                        help="Do not return rawfiles from observations " \
                            "after this MJD.")
    prarse.add_argument('--obssystem-id', dest='obssys_id', \
                        type=int, default=None, \
                        help="Grab rawfiles from a specific observing system. " \
                            "NOTE: the argument should be the obssystem_id " \
                            "from the database. " \
                            "(Default: No constraint on obssystem_id.)")
    prarse.add_argument('--obssystem-name', dest='obssystem_name', \
                        type=int, default=None, \
                        help="Grab rawfiles from a specific observing system. " \
                            "NOTE: the argument should be the name of the " \
                            "observing system as recorded in the database. " \
                            "NOTE: SQL regular expression syntax may be used " \
                            "(Default: No constraint on obs system's name.)")
    parser.add_argument('-t', '--telescope', dest='telescope', \
                        type=str, default=None, \
                        help="Grab rawfiles from specific telescopes. " \
                            "The telescope's _name_ must be used here. " \
                            "NOTE: SQL regular expression syntax may be used " \
                            "(Default: No constraint on telescope name.)")
    parser.add_argument('-f', '--frontend', dest='frontend', \
                        type=str, default=None, \
                        help="Grab rawfiles from specific frontends. " \
                            "NOTE: SQL regular expression syntax may be used " \
                            "(Default: No constraint on frontend name.)")
    parser.add_argument('-b', '--backend', dest='backend', \
                        type=str, default=None, \
                        help="Grab rawfiles from specific backends. " \
                            "NOTE: SQL regular expression syntax may be used " \
                            "(Default: No constraint on backend name.)")
    parser.add_argument('-c', '--clock', dest='clock', \
                        type=str, default=None, \
                        help="Grab rawfiles from specific clocks. " \
                            "NOTE: SQL regular expression syntax may be used " \
                            "(Default: No constraint on clock name.)") 
    parser.add_argument('-v', '--verbose', dest='verbose', \
                        default=False, action='store_true', \
                        help="Be verbose; show the contents of the rawfiles " \
                            "matching the search criteria provided. " \
                            "(Default: Don't be verbose.)")
    args = parser.parse_args()
    main()
