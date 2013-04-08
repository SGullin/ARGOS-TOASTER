#!/usr/bin/env python
"""Create a timfile entry in the database.
"""
import sys
import shlex
import os.path
import textwrap
import warnings
import argparse

import config
import utils
import database
import errors
from toolkit.timfiles import write_timfile as wt
from toolkit.timfiles import conflict_handlers

SHORTNAME = 'create'
DESCRIPTION = 'Extracts TOA information ' \
              'from table, and creates a tim file for ' \
              'use with tempo2.'

CONFLICT_HANDLERS = {'strict': conflict_handlers.strict_conflict_handler, \
                     'tolerant': conflict_handlers.tolerant_conflict_handler, \
                     'newest': conflict_handlers.get_newest_toas}


def add_arguments(parser):
    parser.add_argument('-p', '--psr', dest='pulsar_name', \
                        type=str, default='%', \
                        help='Pulsar name, or alias. NOTE: This option ' \
                            'must be provided.')
    parser.add_argument('-P', '--process-id', dest='process_ids', \
                        type=int, default=[], action='append', \
                        help="A process ID. Multiple instances of " \
                            "these criteria may be provided.")
    parser.add_argument('-t', '--telescope', dest='telescopes', type=str, \
                        default=[], action='append', \
                        help='Telescope name or alias. NOTE: To get TOAs '
                            'from multiple telescopes, provide the ' \
                            '-t/--telescope option multiple times.')
    parser.add_argument('--backend', dest='backends', \
                        type=str, default=[], action='append', \
                        help='Backend name. NOTE: To get TOAs from ' \
                            'multiple backends, provide the --backend ' \
                            'options multiple times.')
    parser.add_argument('--toa-id', dest='toa_ids', \
                        type=int, default=[], action='append', \
                        help='Individual TOA ID to include. Multiple ' \
                            '--toa-id options can be provided.')
    parser.add_argument('--start-mjd', dest='start_mjd', type=float, \
                        help='Get TOAs with MJD larger than this value.')
    parser.add_argument('--end-mjd', dest='end_mjd', type=float, \
                        help='Get TOAs with MJD smaller than this value.')
    parser.add_argument('-m', '--manipulator', dest='manipulators', \
                        type=str, default=[], action='append', \
                        help="Name of manipulator to match. Multiple '-m/" \
                            "--manipulator' arguments may be provided. " \
                            "(Default: match all manipulators).")
    parser.add_argument('--on-conflict', dest='on_conflict', \
                        choices=CONFLICT_HANDLERS, default='strict', \
                        help="Determine what to do when conflicting " \
                            "TOAs are selected. (Default: raise an " \
                            "exception.)")
    parser.add_argument('-n', '--dry-run', dest='dry_run', \
                        action='store_true', default=False, \
                        help="Print information about the timfile, but " \
                            "don't actually create it.")
    parser.add_argument('--comments', dest='comments', \
                        type=str, \
                        help="Provide comments describing the template.")
    parser.add_argument("--from-file", dest='from_file', \
                        type=str, default=None, \
                        help="A file containing a list of command line " \
                            "arguments use.")


def toa_select(args, existdb=None):
    """Return an SQLAlchemy Select object that will fetch
        TOAs given the search criteria provided.

        Inputs:
            args: Arguments from argparse.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            select: The SQLAlchemy select construct.
    """
    db = existdb or database.Database()
    db.connect()
    whereclause = db.pulsar_aliases.c.pulsar_alias.like(args.pulsar_name)
    tmp = False
    for tel in args.telescopes:
        tmp |= (db.telescope_aliases.c.telescope_name.like(tel))
    if tmp:
        whereclause &= (tmp)
    
    tmp = False
    for be in args.backends:
        tmp |= (db.obssystems.c.backend.like(be))
    if tmp:
        whereclause &= (tmp)
    
    if args.manipulators:
        tmp = db.process.c.manipulator.like(args.manipulators[0])
        for manip in args.manipulators[1:]:
            tmp |= (db.process.c.manipulator.like(manip))
        whereclause &= (tmp)
    
    if args.start_mjd is not None:
        whereclause &= ((db.toas.c.imjd+db.toas.c.fmjd) >= args.start_mjd)
    if args.end_mjd is not None:
        whereclause &= ((db.toas.c.imjd+db.toas.c.fmjd) <= args.end_mjd)
    if args.toa_ids:
        whereclause &= (db.toas.c.toa_id.in_(args.toa_ids)) 
    if args.process_ids:
        whereclause &= (db.toas.c.process_id.in_(args.process_ids)) 
    
    
    select = db.select([db.toas.c.toa_id.distinct(), \
                        db.toas.c.process_id, \
                        db.toas.c.rawfile_id, \
                        db.toas.c.pulsar_id, \
                        db.toas.c.obssystem_id, \
                        db.toas.c.imjd, \
                        db.toas.c.fmjd, \
                        (db.toas.c.fmjd+db.toas.c.imjd).label('mjd'), \
                        db.toas.c.freq, \
                        db.toas.c.toa_unc_us, \
                        db.toas.c.bw, \
                        db.toas.c.length, \
                        db.toas.c.nbin, \
                        database.sa.func.ifnull(db.toas.c.goodness_of_fit, 0).\
                                label('goodness_of_fit'), \
                        db.obssystems.c.name.label('obssystem'), \
                        db.obssystems.c.backend, \
                        db.obssystems.c.frontend, \
                        db.obssystems.c.band_descriptor, \
                        db.telescopes.c.telescope_id, \
                        db.telescopes.c.telescope_name, \
                        db.telescopes.c.telescope_abbrev, \
                        db.telescopes.c.telescope_code, \
                        db.process.c.version_id, \
                        db.process.c.template_id, \
                        db.process.c.parfile_id, \
                        db.process.c.add_time, \
                        db.process.c.manipulator, \
                        db.rawfiles.c.filename.label('rawfile'), \
                        db.replacement_rawfiles.c.replacement_rawfile_id, \
                        db.templates.c.filename.label('template'), \
                        (db.toas.c.bw/db.rawfiles.c.bw * \
                                db.rawfiles.c.nchan).label('nchan')], \
                from_obj=[db.toas.\
                    join(db.pulsar_aliases, \
                        onclause=db.toas.c.pulsar_id == \
                                db.pulsar_aliases.c.pulsar_id).\
                    outerjoin(db.pulsars, \
                        onclause=db.pulsar_aliases.c.pulsar_id == \
                                db.pulsars.c.pulsar_id).\
                    outerjoin(db.process, \
                        onclause=db.toas.c.process_id == \
                                db.process.c.process_id).\
                    outerjoin(db.rawfiles, \
                        onclause=db.rawfiles.c.rawfile_id == \
                                db.toas.c.rawfile_id).\
                    outerjoin(db.replacement_rawfiles, \
                        onclause=db.rawfiles.c.rawfile_id == \
                                db.replacement_rawfiles.c.obsolete_rawfile_id).\
                    outerjoin(db.templates, \
                        onclause=db.templates.c.template_id == \
                                db.toas.c.template_id).\
                    outerjoin(db.obssystems, \
                        onclause=db.toas.c.obssystem_id == \
                                db.obssystems.c.obssystem_id).\
                    outerjoin(db.telescopes, \
                        onclause=db.telescopes.c.telescope_id == \
                                db.obssystems.c.telescope_id).\
                    join(db.telescope_aliases, \
                        onclause=db.telescopes.c.telescope_id == \
                                db.telescope_aliases.c.telescope_id)]).\
                where(whereclause)
    return select


def get_toas(args, existdb=None):
    """Return a dictionary of information for each TOA
        in the DB that matches the search criteria provided.

        Inputs:
            args: Arguments from argparser.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            rows: A list of dicts for each matching row.
    """
    db = existdb or database.Database()
    db.connect()
    
    select = toa_select(args, db)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        db.close()
    return rows


def print_summary(toas, comments):
    """Print a summary about the TOAs.

        Inputs:
            toas: A list of row objects each representing a TOA.
            comments: User comments describing the timfile.
        
        Output:
            None
    """
    telescopes = {}
    obssystems = {}
    manipulators = {}
    for toa in toas:
        # Telescopes
        ntel = telescopes.get(toa['telescope_name'], 0) +1
        telescopes[toa['telescope_name']] = ntel
        # Observing systems
        nobssys = obssystems.get(toa['obssystem'], 0) + 1
        obssystems[toa['obssystem']] = nobssys
        # Manpulators
        nman = manipulators.get(toa['manipulator'], 0) + 1
        manipulators[toa['manipulator']] = nman
    print "Number of TOAs: %d" % len(toas)
    print "Number of telescopes: %d" % len(telescopes)
    print "Number of obssystems: %d" % len(obssystems)
    print "Number of manipulators: %d" % len(manipulators)


def add_timfile_entry(toas, cmdline, comments, existdb=None):
    """Insert a timfile entry in the DB, and associate
        TOAs with it.

        Inputs:
            toas: A list of row objects each representing a TOA.
            cmdline: the command line used when running the program.
            comments: User comments describing the timfile.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            timfile_id: The resulting ID of the timfile entry.
    """
    db = existdb or database.Database()
    db.connect()

    # Insert timfile entry
    ins = db.timfiles.insert()
    values = {'user_id':utils.get_userid(), \
              'version_id':utils.get_version_id(db), \
              'comments':comments, \
              'pulsar_id':toas[0]['pulsar_id'], \
              'input_args':cmdline}
    result = db.execute(ins, values)
    timfile_id = result.inserted_primary_key[0]
    result.close()
    
    # Associate the TOAs
    ins = db.toa_tim.insert()
    values = []
    for toa in toas:
        values.append({'timfile_id':timfile_id, \
                       'toa_id':toa['toa_id']})
    db.execute(ins, values)

    if not existdb:
        db.close()

    return timfile_id


def main(args):
    # Check to make sure user provided a comment
    if not args.dry_run and args.comments is None:
        raise errors.BadInputError("A comment describing the timfile is " \
                                    "required!")
    
    if args.from_file is not None:
        # Re-create parser, so we can read arguments from file
        parser = utils.DefaultArguments()
        add_arguments(parser)
        if args.from_file == '-':
            argfile = sys.stdin
        else:
            if not os.path.exists(args.from_file):
                raise errors.FileError("The list of cmd line args (%s) " \
                            "does not exist." % args.from_file)
            argfile = open(args.from_file, 'r')
        for line in argfile:
            # Strip comments
            line = line.partition('#')[0].strip()
            if not line:
                # Skip empty line
                continue
            arglist = shlex.split(line.strip())
            args = parser.parse_args(arglist, namespace=args)

    # Establish a database connection
    db = database.Database()
    db.connect()

    trans = db.begin()
    try:
        cmdline = " ".join(sys.argv)
        toas = get_toas(args, db)
        # Check for / handle conflicts
        conflict_handler = CONFLICT_HANDLERS[args.on_conflict]
        toas = conflict_handler(toas)
        if not toas:
            raise errors.ToasterError("No TOAs match criteria provided!") 
        if config.debug.TIMFILE:
            wt.write_timfile(toas, {'comments': args.comments, \
                                    'user_id': utils.get_userid(), \
                                    'add_time': "Not in DB!", \
                                    'timfile_id': -1})
        elif args.dry_run:
            print_summary(toas, args.comments)
        else:
            timfile_id = add_timfile_entry(toas, cmdline, args.comments)
            utils.print_info("Created new timfile entry - timfile_id=%d (%s)" % \
                    (timfile_id, utils.Give_UTC_now()), 1)
    except:
        db.rollback()
        db.close()
        raise
    else:
        db.commit()
        db.close()
    

if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args=parser.parse_args()
    main(args)
