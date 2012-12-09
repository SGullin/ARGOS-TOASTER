#!/usr/bin/env python

"""
This TOASTER utility script provides the user with a listing
of template_id values from the database to help the user choose which
input is most appropriate.

Patrick Lazarus, Apr. 8, 2012.
"""

import argparse
import os.path
import datetime
import warnings

import config
import utils
import database
import errors
import colour


SHORTNAME = 'show'
DESCRIPTION = "Query the database for template information." \


def add_arguments(parser):
    parser.add_argument('--template-id', dest='template_id', \
                        type=int, default=None, \
                        help="A template ID. This is useful for checking " \
                            "the details of a single template, identified " \
                            "by its ID number. NOTE: No other templates " \
                            "will match if this option is provided.")
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


def main(args):
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
    db = database.Database()
    db.connect()

    whereclause = db.pulsar_aliases.c.pulsar_alias.like(args.pulsar_name)
    if args.template_id is not None:
        whereclause &= (db.templates.c.template_id==args.template_id)
    if args.start_date is not None:
        whereclause &= (db.templates.c.add_time >= args.start_date)
    if args.end_date is not None:
        whereclause &= (db.templates.c.add_time <= args.end_date)
    if args.ids:
        whereclause &= (db.templates.c.template_id.in_(args.ids))
    if args.obssys_id:
        whereclause &= (db.obssystems.c.obssystem_id==args.obssys_id)
    if args.obssystem_name:
        whereclause &= (db.obssystems.c.name.like(args.obssystem_name))
    if args.telescope:
        whereclause &= (db.telescope_aliases.c.telescope_alias.\
                                like(args.telescope))
    if args.frontend:
        whereclause &= (db.obssystems.c.frontend.like(args.frontend))
    if args.backend:
        whereclause &= (db.obssystems.c.backend.like(args.backend))
    if args.clock:
        whereclause &= (db.obssystems.c.clock.like(args.clock))
    if args.nbin:
        whereclause &= (db.templates.c.nbin==args.nbin)

    select = db.select([db.templates.c.template_id, \
                        db.templates.c.add_time, \
                        db.templates.c.filename, \
                        db.templates.c.filepath, \
                        db.templates.c.nbin, \
                        db.templates.c.comments, \
                        db.master_templates.c.template_id.label('mtempid'), \
                        db.users.c.real_name, \
                        db.users.c.email_address, \
                        db.pulsars.c.pulsar_name, \
                        db.telescopes.c.telescope_name, \
                        db.obssystems.c.obssystem_id, \
                        db.obssystems.c.name.label('obssys_name'), \
                        db.obssystems.c.frontend, \
                        db.obssystems.c.backend, \
                        db.obssystems.c.clock], \
                from_obj=[db.templates.\
                    join(db.pulsar_aliases, \
                        onclause=db.templates.c.pulsar_id == \
                                db.pulsar_aliases.c.pulsar_id).\
                    outerjoin(db.pulsars, \
                        onclause=db.pulsar_aliases.c.pulsar_id == \
                                db.pulsars.c.pulsar_id).\
                    outerjoin(db.obssystems, \
                        onclause=db.templates.c.obssystem_id == \
                                db.obssystems.c.obssystem_id).\
                    outerjoin(db.master_templates, \
                        onclause=db.master_templates.c.template_id == \
                                db.templates.c.template_id).\
                    outerjoin(db.telescopes, \
                        onclause=db.telescopes.c.telescope_id == \
                                db.obssystems.c.telescope_id).\
                    outerjoin(db.users, \
                        onclause=db.users.c.user_id == \
                                db.templates.c.user_id).\
                    join(db.telescope_aliases, \
                        onclause=db.telescopes.c.telescope_id == \
                                db.telescope_aliases.c.telescope_id)],
                distinct=db.templates.c.template_id).\
                where(whereclause)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    db.close()
    return rows


def show_templates(templates):
    if len(templates):
        print "--"*25
        for tdict in templates:
            print colour.cstring("Template ID:", underline=True, bold=True) + \
                    colour.cstring(" %d" % tdict['template_id'], bold=True)
            fn = os.path.join(tdict['filepath'], tdict['filename'])
            print "\nTemplate: %s" % fn
            print "Pulsar name: %s" % tdict['pulsar_name']
            print "Master template? %s" % \
                    (((tdict['mtempid'] is not None) and "Yes") or "No")
            print "Number of phase bins: %d" % tdict['nbin']
            print "Uploaded by: %s (%s)" % (tdict['real_name'], \
                                            tdict['email_address'])
            print "Uploader's comments: %s" % tdict['comments']
            print "Date and time template was added: %s" % \
                                tdict['add_time'].isoformat(' ')

            # Show extra information if verbosity is >= 1
            lines = ["Observing System ID: %d" % tdict['obssystem_id'], \
                     "Observing System Name: %s" % tdict['obssys_name'], \
                     "Telescope: %s" % tdict['telescope_name'], \
                     "Frontend: %s" % tdict['frontend'], \
                     "Backend: %s" % tdict['backend'], \
                     "Clock: %s" % tdict['clock']]
            utils.print_info("\n".join(lines), 1)
            
            # Show the template if verbosity is >= 2
            cmd = "psrtxt %s" % fn
            psrtxtout, stderr = utils.execute(cmd)

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
            plot, stderr = utils.execute("gnuplot", stdinstr=gnuplotcode)
            utils.print_info(plot, 2)
            print "--"*25
    else:
        raise errors.ToasterError("No templates match parameters provided!")


if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
