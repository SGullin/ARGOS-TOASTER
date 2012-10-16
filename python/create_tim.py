#!/usr/bin/env python

import sys
import os.path
import textwrap

import epta_pipeline_utils as epu
import database
import errors


def check_toas(args, existdb=None):
    """Check to see if there are any conflicts between TOAs.

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
    select.append_group_by(db.toas.c.rawfile_id)
    select.append_having(database.sa.func.count(db.toas.c.process_id.\
                                                distinct()) > 1)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        db.close()

    if len(rows):
        raise errors.ConflictingToasError("TOAs from the same raw file, " \
                                    "but different processing runs match " \
                                    "your selection criteria. This is not " \
                                    "allowed. Be more specific, or use " \
                                    "'--on-conflict=newest'.")
    

def get_newest_toas(args, existdb=None):
    """Get TOAs. If there are conflicts take TOAs from the
        most recent processing job.

        Inputs:
            args: Arguments from argparser.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            rows: A list of dicts for each matching row.
    """
    db = existdb or database.Database()
    db.connect()

    select = db.select([db.process.c.rawfile_id, \
                        database.sa.func.max(db.process.c.add_time).\
                                    label('max_add')]).\
                group_by(db.process.c.rawfile_id)
    alias = database.sa.sql.subquery('newest', [select])
    select = toa_select(args, db)
    select.append_from(alias)
    select.append_whereclause(alias.c.max_add==db.process.c.add_time)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        db.close()
    return rows


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
    
    if args.start_mjd is not None:
        whereclause &= ((db.toas.c.imjd+db.toas.c.fmjd) >= args.start_mjd)
    if args.end_mjd is not None:
        whereclause &= ((db.toas.c.imjd+db.toas.c.fmjd) <= args.end_mjd)
    if args.toa_ids:
        whereclause &= (db.toas.c.toa_id.in_(args.toa_ids)) 
    
    select = db.select([db.toas.c.toa_id.distinct().label('foo'), \
                        db.toas, \
                        db.obssystems.c.name.label('obssystem'), \
                        db.obssystems.c.backend, \
                        db.obssystems.c.frontend, \
                        db.obssystems.c.band_descriptor, \
                        db.telescopes.c.telescope_name, \
                        db.telescopes.c.telescope_abbrev, \
                        db.telescopes.c.telescope_code, \
                        db.process.c.version_id, \
                        db.process.c.add_time, \
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
    
    check_toas(args, db)
    select = toa_select(args, db)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        db.close()
    return rows


def create_timfile(toas, cmdline, comments, existdb=None):
    """Create a timfile entry in the DB, and associate
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
    values = {'user_id':epu.get_current_users_id(db), \
              'version_id':epu.get_version_id(db), \
              'comments':comments, \
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


def write_timfile(toas, flags, comments, outname):
    """Write TOAs to a timfile.
        
        Inputs:
            toas: A list of TOAs.
            flags: A list of flags to add to each TOA.
            comments: A user comment describing the timfile.
            outname: The output file's name.

        Outputs:
            None
    """
    if os.path.exists(outname):
        raise errors.FileError("The output timfile sepcified (%s) " \
                                "already exists. Doing nothing...")
    tim = open(outname, 'w')

    wrapper = textwrap.TextWrapper(initial_indent="# ", \
                                   subsequent_indent="# ")
    tim.write(wrapper.fill(comments)+'\n')
    tim.write("# Automatically generated by TOASTER's '%s'\n" % __file__)
    # tim.write("# Created by: %s (%s)\n" % NotImplemented)
    tim.write("# Created at: %s\n" % epu.Give_UTC_now())
    for toa in toas:
        fmjdstr = str(toa['fmjd'])
        mjd = "%s%s" % (toa['imjd'], fmjdstr[fmjdstr.index('.'):])
        toastr = "%s %.3f %s %.3f %s" % \
                    (toa['rawfile'], toa['freq'], mjd, \
                        toa['toa_unc_us'], toa['telescope_code'])
        flagstr = flags % toa
        tim.write("%s %s\n" % (toastr, flagstr))
    tim.close()
    epu.print_info("Successfully wrote %d TOAs to timfile (%s)" % \
                    (len(toas), outname), 1)


def main():
    db = database.Database()
    db.connect()

    trans = db.begin()
    try:
        cmdline = " ".join(sys.argv)
        toa_getter = toa_getters[args.on_conflict]
        toas = toa_getter(args, db)
        timfile_id = create_timfile(toas, cmdline, args.comments)
        epu.print_info("Created new timfile entry - timfile_id=%d (%s)" % \
                (timfile_id, epu.Give_UTC_now()), 1)
        # Write TOAs
        write_timfile(toas, args.flags, args.comments, args.outname)
    except:
        db.rollback()
        db.close()
        raise
    else:
        db.commit()
        db.close()
    

if __name__=='__main__':
    toa_getters = {'raise': get_toas, \
                   'newest': get_newest_toas}

    parser = epu.DefaultArguments(description='Extracts TOA information ' \
                                'from table, and creates a tim file for '
                                'use with tempo2.')
    parser.add_argument('-p', '--pulsar', dest='pulsar_name', \
                        required=True, type=str, \
                        help='Pulsar name, or alias. NOTE: This option ' \
                            'must be provided.')
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
    #parser.add_argument('--fmt', dest='format', type=str, \
    #                    default='tempo2', \
    #                    help="Output file format. NOTE: Only 'tempo2' is " \
    #                        "currently supported. (Default: tempo2)")
    flags = parser.add_mutually_exclusive_group(required=False)
    flags.add_argument('--flags', dest='flags', type=str, \
                        default='', \
                        help="Flags to include for each TOA. Both the " \
                            "flag and value-tag should be included in a " \
                            "quoted string. Value-tags should be in " \
                            "%%(<tag-name>)<fmt> format. Where <tag-name> " \
                            "is the name of the column in the DB, and " \
                            "<fmt> is a C/python-style format code " \
                            "(without the leading '%%'). NOTE: If multiple " \
                            "flags are desired they should all be included " \
                            "in the same quoted string. (Default: no flags)")
    flags.add_argument('--ipta-exchange', dest='flags', action='store_const', \
                        default='', \
                        const="-fe %(frontend)s -be %(backend)s " \
                            "-B %(band_descriptor)s -bw %(bw).1f " \
                            "-tobs %(length).1f -pta EPTA " \
                            "-proc EPTA_pipeline_verID%(version_id)d " \
                            "-tmplt %(template)s -gof %(goodness_of_fit).3f " \
                            "-nbin %(nbin)d -nch %(nchan)d " \
                            "-f %(frontend)s_%(backend)s", \
                        help="Set flags appropriate for the IPTA exchange " \
                            "format.")
    parser.add_argument('--on-conflict', dest='on_conflict', \
                        choices=toa_getters, default='raise', \
                        help="Determine what to do when conflicting " \
                            "TOAs are selected. (Default: raise an " \
                            "exception.)")
    parser.add_argument('--comments', dest='comments', \
                        required=True, type=str, \
                        help="Provide comments describing the template.")
    parser.add_argument('-o', '--outname', dest='outname', \
                        required=True, type=str, \
                        help="Output timfile's name. NOTE: This is "
                            "required.")
    args=parser.parse_args()
    main()
