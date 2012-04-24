#!/usr/bin/env python2.6

"""
This EPTA Pipeline utility script provides the user with a listing
of template_id values from the database to help the user choose which
input is most appropriate.

Patrick Lazarus, Apr. 8, 2012.
"""

import argparse
import os.path
import datetime
import warnings

import epta_pipeline_utils as epu
import database
import errors
import colour
import config


def main():
    templates = get_templates(args)
    show_templates(templates)


def get_templates(args):
    """Return a dictionary of information for each template
        in the DB that matches the search criteria provided.

        Input:
            args: Arugments from argparser.

        Output:
            rows: A list of dicts for each matching row. 
    """
    query = "SELECT t.template_id, " \
                   "t.add_time, " \
                   "t.filename, " \
                   "t.filepath, " \
                   "t.nbin, " \
                   "t.comments, " \
                   "t.is_analytic, " \
                   "IFNULL(mt.template_id, 0) AS is_master, " \
                   "u.real_name, " \
                   "u.email_address, " \
                   "psr.pulsar_name, " \
                   "tel.name AS telescope_name, " \
                   "obs.obssystem_id, " \
                   "obs.name AS obssys_name, " \
                   "obs.frontend, " \
                   "obs.backend, " \
                   "obs.clock " \
            "FROM templates AS t " \
            "LEFT JOIN pulsars AS psr " \
                "ON psr.pulsar_id=t.pulsar_id " \
            "LEFT JOIN obssystems AS obs " \
                "ON obs.obssystem_id=t.obssystem_id " \
            "LEFT JOIN master_templates AS mt " \
                "ON mt.template_id=t.template_id " \
            "LEFT JOIN telescopes AS tel " \
                "ON tel.telescope_id=obs.telescope_id " \
            "LEFT JOIN users AS u " \
                "ON u.user_id=t.user_id " \
            "WHERE (psr.pulsar_name LIKE %s) "
    query_args = [args.pulsar_name]

    if args.start_date is not None:
        query += "AND t.add_time >= %s "
        query_args.append(args.start_date)
    if args.end_date is not None:
        query += "AND t.add_time <= %s "
        query_args.append(args.end_date)
    if args.ids:
        query += "AND t.template_id IN %s "
        query_args.append(args.ids)
    if args.obssys_id:
        query += "AND (obs.obssystem_id = %s) "
        query_args.append(args.obssys_id)
    if args.obssystem_name:
        query += "AND (obs.name LIKE %s) "
        query_args.append(args.obssystem_name)
    if args.telescope:
        query += "AND (tel.name LIKE %s) "
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
    if args.nbin:
        query += "AND (t.nbin=%s) "
        query_args.append(args.nbin)
    if args.only_analytic:
        query += "AND (t.is_analytic) "
    if args.no_analytic:
        query += "AND (NOT t.is_analytic) "

    db = database.Database()
    db.execute(query, query_args)
    templates = db.fetchall()
    db.close()
    return templates


def show_templates(templates):
    if len(templates):
        for tdict in templates:
            print "- "*25
            print colour.cstring("Template ID:", underline=True, bold=True) + \
                    colour.cstring(" %d" % tdict.template_id, bold=True)
            fn = os.path.join(tdict.filepath, tdict.filename)
            print "\nTemplate: %s" % fn
            print "Pulsar name: %s" % tdict.pulsar_name
            print "Master template? %s" % (tdict.is_master and "Yes" or "No")
            print "Template type: %s" % (tdict.is_analytic and "Analytic" or "Non-analytic")
            if not tdict.is_analytic:
                print "Number of phase bins: %d" % tdict.nbin
            print "Uploaded by: %s (%s)" % (tdict.real_name, tdict.email_address)
            print "Uploader's comments: %s" % tdict.comments
            print "Date and time template was added: %s" % tdict.add_time.isoformat(' ')

            # Show extra information if verbosity is >= 1
            lines = ["Observing System ID: %d" % tdict.obssystem_id, \
                     "Observing System Name: %s" % tdict.obssys_name, \
                     "Telescope: %s" % tdict.telescope_name, \
                     "Frontend: %s" % tdict.frontend, \
                     "Backend: %s" % tdict.backend, \
                     "Clock: %s" % tdict.clock]
            epu.print_info("\n".join(lines), 1)
            
            # Show the template if verbosity is >= 2
            if tdict.is_analytic:
                f = open(fn, 'r')
                comps = [[float(c) for c in line.split()] for line in f.readlines()]
                f.close()
                lines = ["Number of components: %d" % len(comps)]
                for ii, (phs, con, hgt) in enumerate(comps):
                    lines.append("Component #%d: Phase=%g, Concentration=%g, " \
                                    "Height=%g" % (ii+1, phs, con, hgt))
            else:
                cmd = "psrtxt %s" % fn
                psrtxtout, stderr = epu.execute(cmd)

                gnuplotcode = """set term dumb
                                 set format y ""
                                 set nokey
                                 set border 1
                                 set tics out
                                 set xtics nomirror
                                 set ytics 0,1,0
                                 set xlabel "Phase Bin"
                                 set xrange [0:%d]
                                 plot "-" using 3:4 w l
                                 %s
                                 end
                             """ % (tdict.nbin-1, psrtxtout)
                plot, stderr = epu.execute("gnuplot", stdinstr=gnuplotcode)
            epu.print_info(plot, 2)
            print " -"*25
    else:
        raise errors.EptaPipelineError("No templates match parameters provided!")


if __name__=='__main__':
    parser = epu.DefaultArguments(description="Get a listing of tempalte_id " \
                                        "values from the DB to help the user" \
                                        "find the appropriate one to use.")
    parser.add_argument('-p', '--psr', dest='pulsar_name', \
                        type=str, default='%', \
                        help="The pulsar to grab templates for. " \
                            "NOTE: SQL regular expression syntax may be used")
    parser.add_argument('-s', '--start-date', dest='start_date', \
                        type=str, default=None, \
                        help="Do not return templates added to the DB " \
                            "before this date.")
    parser.add_argument('-e', '--end-date', dest='end_date', \
                        type=str, default=None, \
                        help="Do not return templates added to the DB " \
                            "after this date.")
    parser.add_argument('-i', '--id', dest='ids', type=int, \
                        default=[], action='append', \
                        help="Specific template_id numbers to describe.")
    parser.add_argument('-n', '--nbin', dest='nbin', \
                        type=int, default=None, \
                        help="Only show templates with a specific number " \
                            "of bins. NOTE: this will exclude analytic " \
                            "templates.")
    parser.add_argument('--only-analytic', dest='only_analytic', \
                        action='store_true', \
                        help="Only show analytic templates.")
    parser.add_argument('--no-analytic', dest='no_analytic', \
                        action='store_true', \
                        help="Do not show analytic templates.")
    parser.add_argument('--obssystem-id', dest='obssys_id', \
                        type=int, default=None, \
                        help="Grab templates from a specific observing system. " \
                            "NOTE: the argument should be the obssystem_id " \
                            "from the database. " \
                            "(Default: No constraint on obssystem_id.)")
    parser.add_argument('--obssystem-name', dest='obssystem_name', \
                        type=int, default=None, \
                        help="Grab templates from a specific observing system. " \
                            "NOTE: the argument should be the name of the " \
                            "observing system as recorded in the database. " \
                            "NOTE: SQL regular expression syntax may be used " \
                            "(Default: No constraint on obs system's name.)")
    parser.add_argument('-t', '--telescope', dest='telescope', \
                        type=str, default=None, \
                        help="Grab templates from specific telescopes. " \
                            "The telescope's _name_ must be used here. " \
                            "NOTE: SQL regular expression syntax may be used " \
                            "(Default: No constraint on telescope name.)")
    parser.add_argument('-f', '--frontend', dest='frontend', \
                        type=str, default=None, \
                        help="Grab templates from specific frontends. " \
                            "NOTE: SQL regular expression syntax may be used " \
                            "(Default: No constraint on frontend name.)")
    parser.add_argument('-b', '--backend', dest='backend', \
                        type=str, default=None, \
                        help="Grab templates from specific backends. " \
                            "NOTE: SQL regular expression syntax may be used " \
                            "(Default: No constraint on backend name.)")
    parser.add_argument('-c', '--clock', dest='clock', \
                        type=str, default=None, \
                        help="Grab templates from specific clocks. " \
                            "NOTE: SQL regular expression syntax may be used " \
                            "(Default: No constraint on clock name.)") 
    args = parser.parse_args()
    main()
