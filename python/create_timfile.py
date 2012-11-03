#!/usr/bin/env python
"""Create a timfile entry in the database.
"""
import sys
import os.path
import textwrap
import warnings

import utils
import database
import errors


def strict_conflict_handler(toas):
    """Check to see if there are any conflicts between TOAs.
        Raise an error if there is.

        Input:
            toas: The list of TOAs (ie rows returned from the DB)

        Outputs:
            toas: The input list of TOAs. (Only if no conflict is
                found).
    """
    # Collect information to see if there are any conflicts
    rawfile_ids = {}
    obssystem_ids = {}
    pulsar_ids = {}
    for toa in toas:
        rawfile_ids.setdefault(toa['rawfile_id'], set()).\
                                add(toa['process_id'])
        obssystem_ids.setdefault(toa['obssystem_id'], set()).\
                                add(toa['template_id'])
        pulsar_ids.setdefault(toa['pulsar_id'], set()).\
                                add(toa['parfile_id'])
    # Respond to any conflicts
    for procids in rawfile_ids.values():
        if len(procids) > 1:
            raise errors.ConflictingToasError("Some TOAs come from the same " \
                                "data file, but different processing jobs!")
    for tempids in obssystem_ids.values():
        if len(tempids) > 1:
            raise errors.ConflictingToasError("Some TOAs are from the same " \
                                "observing system, but have been generated " \
                                "with different templates!")
    for parids in pulsar_ids.values():
        if len(parids) > 1:
            raise errors.ConflictingToasError("Some TOAs are from the same " \
                                "pulsar, but have a different ephemeris " \
                                "installed!")
    return toas 


def tolerant_conflict_handler(toas):
    """Check to see if there are any TOAs from the same data file,
        but different processing runs.
        Raise an error if there is.

        Input:
            toas: The list of TOAs (ie rows returned from the DB)

        Outputs:
            toas: The input list of TOAs. (Only if no conflict is
                found).
    """
    # Collect information to see if there are any conflicts
    rawfile_ids = {}
    obssystem_ids = {}
    pulsar_ids = {}
    for toa in toas:
        rawfile_ids.setdefault(toa['rawfile_id'], set()).\
                                add(toa['process_id'])
        obssystem_ids.setdefault(toa['obssystem_id'], set()).\
                                add(toa['template_id'])
        pulsar_ids.setdefault(toa['pulsar_id'], set()).\
                                add(toa['parfile_id'])
    # Respond to any conflicts
    for procids in rawfile_ids.values():
        if len(procids) > 1:
            raise errors.ConflictingToasError("Some TOAs come from the same " \
                                "data file, but different processing jobs!")
    for tempids in obssystem_ids.values():
        if len(tempids) > 1:
            warnings.warn("Some TOAs are from the same observing " \
                          "system, but have been generated with " \
                          "different templates!", errors.ToasterWarning)
    for parids in pulsar_ids.values():
        if len(parids) > 1:
            warnings.warn("Some TOAs are from the same " \
                          "pulsar, but have a different ephemeris " \
                          "installed!", errors.ToasterWarning)
    return toas 


def get_newest_toas(toas):
    """Get TOAs. If there are conflicts take TOAs from the
        most recent processing job.

        Inputs:
            toas: The list of TOAs (ie rows returned from the DB)

        Outputs:
            toas: The (*in-place*) modified list of input TOAs, 
                using only those from the most recent processing 
                run for each data file.
    """
    # Sort TOAs by when they were produced
    toas.sort(key=lambda x: x['add_time'], reverse=True)
    rawfile_ids = {}
    obssystem_ids = {}
    pulsar_ids = {}
    ii = 0
    while ii < len(toas):
        toa = toas[ii]
        procids = rawfile_ids.setdefault(toa['rawfile_id'], set())
        if len(procids) and toa['process_id'] not in procids:
            toas.pop(ii)
            continue
        else:
            procids.add(toa['process_id'])
            ii += 1
        obssystem_ids.setdefault(toa['obssystem_id'], set()).\
                                add(toa['template_id'])
        pulsar_ids.setdefault(toa['pulsar_id'], set()).\
                                add(toa['parfile_id'])
    # Warn if other minor conflicts were found
    for tempids in obssystem_ids.values():
        if len(tempids) > 1:
            warnings.warn("Some TOAs are from the same observing " \
                          "system, but have been generated with " \
                          "different templates!", errors.ToasterWarning)
    for parids in pulsar_ids.values():
        if len(parids) > 1:
            warnings.warn("Some TOAs are from the same " \
                          "pulsar, but have a different ephemeris " \
                          "installed!", errors.ToasterWarning)
    return toas


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


def main():
    # Check to make sure user provided a comment
    if not args.dry_run and args.comments is None:
        raise errors.BadInputError("A comment describing the timfile is " \
                                    "required!")
    db = database.Database()
    db.connect()

    trans = db.begin()
    try:
        cmdline = " ".join(sys.argv)
        toas = get_toas(args, db)
        # Check for / handle conflicts
        conflict_handler = conflict_handlers[args.on_conflict]
        toas = conflict_handler(toas)
        if not toas:
            raise errors.ToasterError("No TOAs match criteria provided!") 
        if args.dry_run:
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
    conflict_handlers = {'strict': strict_conflict_handler, \
                         'tolerant': tolerant_conflict_handler, \
                         'newest': get_newest_toas}

    parser = utils.DefaultArguments(description='Extracts TOA information ' \
                                'from table, and creates a tim file for '
                                'use with tempo2.')
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
                        choices=conflict_handlers, default='strict', \
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
    args=parser.parse_args()
    main()
