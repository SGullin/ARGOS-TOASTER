#!/usr/bin/env python

"""
This EPTA Pipeline utility script provides the user with a listing
of rawfile_id values from the database to help the user choose which
input is most appropriate.

Patrick Lazarus, Jan. 8, 2012.
"""

import os.path
import warnings

import epta_pipeline_utils as epu
import database
import errors
import colour

def main():
    rawfiles = get_rawfiles(args)
    show_rawfiles(rawfiles)


def get_rawfiles(args):
    """Return a dictionary of information for each rawfile
        in the DB that matches the search criteria provided.

        Input:
            args: Arugments from argparser.

        Output:
            rows: A list of dicts for each matching row. 
    """
    db = database.Database()
    db.connect()

    whereclause = db.pulsar_aliases.c.pulsar_alias.like(args.pulsar_name)
    if args.rawfile_id is not None:
        whereclause &= (db.rawfiles.c.rawfile_id==args.rawfile_id)
    if args.start_date is not None:
        whereclause &= (db.rawfiles.c.add_time >= args.start_date)
    if args.end_date is not None:
        whereclause &= (db.rawfiles.c.add_time <= args.end_date)
    if args.start_mjd is not None:
        whereclause &= (db.rawfiles.c.mjd >= args.start_mjd)
    if args.end_mjd is not None:
        whereclause &= (db.rawfiles.c.mjd <= args.end_mjd)
    if args.obssys_id:
        whereclause &= (db.obssystems.c.obssystem_id==args.obssys_id)
    if args.obssystem_name:
        whereclause &= (db.obssystems.c.name.like(args.obssystem_name))
    if args.telescope:
        whereclause &= (db.telescope_aliases.c.telescope_name.like(args.telescope))
    if args.frontend:
        whereclause &= (db.obssystems.c.frontend.like(args.frontend))
    if args.backend:
        whereclause &= (db.obssystems.c.backend.like(args.backend))
    if args.clock:
        whereclause &= (db.obssystems.c.clock.like(args.clock))
    
    select = db.select([db.rawfiles.c.rawfile_id, \
                        db.rawfiles.c.add_time, \
                        db.rawfiles.c.filename, \
                        db.rawfiles.c.filepath, \
                        db.rawfiles.c.nbin, \
                        db.rawfiles.c.nchan, \
                        db.rawfiles.c.npol, \
                        db.rawfiles.c.nsub, \
                        db.rawfiles.c.freq, \
                        db.rawfiles.c.bw, \
                        db.rawfiles.c.dm, \
                        db.rawfiles.c.length, \
                        db.rawfiles.c.mjd, \
                        db.users.c.real_name, \
                        db.users.c.email_address, \
                        db.pulsars.c.pulsar_name, \
                        db.telescopes.c.telescope_name, \
                        db.obssystems.c.obssystem_id, \
                        db.obssystems.c.name.label('obssys_name'), \
                        db.obssystems.c.frontend, \
                        db.obssystems.c.backend, \
                        db.obssystems.c.clock], \
                from_obj=[db.rawfiles.\
                   join(db.pulsar_aliases, \
                        onclause=db.rawfiles.c.pulsar_id == \
                                db.pulsar_aliases.c.pulsar_id).\
                    outerjoin(db.pulsars, \
                        onclause=db.pulsar_aliases.c.pulsar_id == \
                                db.pulsars.c.pulsar_id).\
                    outerjoin(db.obssystems, \
                        onclause=db.rawfiles.c.obssystem_id == \
                                db.obssystems.c.obssystem_id).\
                    outerjoin(db.telescopes, \
                        onclause=db.telescopes.c.telescope_id == \
                                db.obssystems.c.telescope_id).\
                    outerjoin(db.users, \
                        onclause=db.users.c.user_id == \
                                db.rawfiles.c.user_id).\
                    join(db.telescope_aliases, \
                        onclause=db.telescopes.c.telescope_id == \
                                db.telescope_aliases.c.telescope_id)],
                distinct=db.rawfiles.c.rawfile_id).\
                where(whereclause)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    db.close()
    return rows 


def show_rawfiles(rawfiles):
    if len(rawfiles):
        for rawdict in rawfiles:
            print "- "*25
            print colour.cstring("Rawfile ID:", underline=True, bold=True) + \
                    colour.cstring(" %d" % rawdict.rawfile_id, bold=True)
            fn = os.path.join(rawdict.filepath, rawdict.filename)
            print "\nRawfile: %s" % fn
            print "Pulsar name: %s" % rawdict.pulsar_name
            print "Uploaded by: %s (%s)" % \
                        (rawdict.real_name, rawdict.email_address)
            print "Date and time rawfile was added: %s" % rawdict.add_time.isoformat(' ')
            lines = ["Observing System ID: %d" % rawdict.obssystem_id, \
                     "Observing System Name: %s" % rawdict.obssys_name, \
                     "Telescope: %s" % rawdict.telescope_name, \
                     "Frontend: %s" % rawdict.frontend, \
                     "Backend: %s" % rawdict.backend, \
                     "Clock: %s" % rawdict.clock]
            epu.print_info("\n".join(lines), 1)
            lines = ["MJD: %.6f" % rawdict.mjd, \
                     "Number of phase bins: %d" % rawdict.nbin, \
                     "Number of channels: %d" % rawdict.nchan, \
                     "Number of polarisations: %d" % rawdict.npol, \
                     "Number of sub-integrations: %d" % rawdict.nsub, \
                     "Centre frequency (MHz): %g" % rawdict.freq, \
                     "Bandwidth (MHz): %g" % rawdict.bw, \
                     "Dispersion measure (pc cm^-3): %g" % rawdict.dm, \
                     "Integration time (s): %g" % rawdict.length]
            epu.print_info("\n".join(lines), 2)
            print " -"*25
    else:
        raise errors.EptaPipelineError("No rawfiles match parameters provided!")


if __name__=='__main__':
    parser = epu.DefaultArguments(description="Get a listing of rawfile_id " \
                                        "values from the DB to help the user " \
                                        "find the appropriate one to use.")
    parser.add_argument('-r', '--rawfile-id', dest='rawfile_id', \
                        type=int, default=None, \
                        help="A raw file ID. This is useful for checking " \
                            "the details of a single raw file, identified " \
                            "by its ID number. NOTE: No other raw files " \
                            "will match if this option is provided.")
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
    parser.add_argument('--obssystem-id', dest='obssys_id', \
                        type=int, default=None, \
                        help="Grab rawfiles from a specific observing system. " \
                            "NOTE: the argument should be the obssystem_id " \
                            "from the database. " \
                            "(Default: No constraint on obssystem_id.)")
    parser.add_argument('--obssystem-name', dest='obssystem_name', \
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
    args = parser.parse_args()
    main()
