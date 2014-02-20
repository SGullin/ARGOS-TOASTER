#!/usr/bin/env python
"""Write a timfile from an entry in the database.
"""
import os.path
import sys
import textwrap

from toaster import database
from toaster import errors
from toaster import utils
from toaster.utils import notify
from toaster.utils import cache
from toaster.toolkit.timfiles import formatters

SHORTNAME = 'write'
DESCRIPTION = 'Writes out a tim file ' \
              'already defined in the DB. The output ' \
              'format is suitable for TEMPO2.'

# Formatter functions
FORMATTERS = {'tempo2': formatters.tempo2_formatter,
              'princeton': formatters.princeton_formatter}


def add_arguments(parser):
    parser.add_argument('-t', '--timfile-id', dest='timfile_id',
                        required=True, type=int,
                        help="The ID of the timefile entry in the DB to "
                             "write out. NOTE: This is required.")
    parser.add_argument('-o', '--outname', dest='outname',
                        default='-', type=str,
                        help="Output timfile's name. NOTE: This is "
                             "required.")
    parser.add_argument('-f', '--format', dest='format',
                        default='tempo2', type=str,
                        help="Output format for the timfile. "
                             "Available formats: '%s'. (Default: "
                             "tempo2)" % "', '".join(sorted(FORMATTERS)))
    flags = parser.add_mutually_exclusive_group(required=False)
    flags.add_argument('-F', '--flag', dest='flags', action='append',
                       default=[], nargs=2, type=str,
                       help="A flag to include for each TOA. Two arguments "
                            "are required: The first is the name of the flag; "
                            "the second is the value-tag. Value-tags may be in "
                            "%%(<tag-name>)<fmt> format. Where <tag-name> "
                            "is the name of the column in the DB, and "
                            "<fmt> is a C/python-style format code "
                            "(without the leading '%%'). Multiple instances "
                            "of -F/--flag may be provided to include "
                            "multiple flags per TOA. (Default: no flags)")
    flags.add_argument('--ipta-exchange', dest='flags', action='store_const',
                       default=[],
                       const=[("fe", "%(frontend)s"),
                              ("be", "%(backend)s"),
                              ("B", "%(band_descriptor)s"),
                              ("bw", "%(bw).1f"),
                              ("tobs", "%(length).1f"),
                              ("proc", "TOASTER_verID%(version_id)d"),
                              ("tmplt", "%(template)s"),
                              ("gof", "%(goodness_of_fit).3f"),
                              ("nbin", "%(nbin)d"),
                              ("nch", "%(nchan)d"),
                              ("f", "%(frontend)s_%(backend)s")],
                       help="Set flags appropriate for the IPTA exchange "
                            "format.")
    parser.add_argument('--sort', dest='sortkeys', metavar='SORTKEY',
                        action='append', default=['mjd', 'freq'],
                        help="DB column to sort TOAs by. Multiple "
                             "--sort options can be provided. Options "
                             "provided later will take precedent "
                             "over previous options. (Default: Sort "
                             "by MJD, then freq.)")


def get_timfile(timfile_id, existdb=None):
    """Get a timfile's comments and TOAs.

        Input:
            timfile_id: The ID of the timfile to get TOAs for.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            toas: A list of TOAs.
            timfile_info: The information about the timfile.
    """
    db = existdb or database.Database()
    db.connect()

    # Get information about the timfile
    select = db.select([db.timfiles]).\
                where(db.timfiles.c.timfile_id == timfile_id)
    result = db.execute(select)
    row = result.fetchone()
    result.close()

    if not row:
        raise errors.DatabaseError("There is no timfile with ID=%d" %
                                   timfile_id)

    toas = get_toas(timfile_id, existdb)

    if not existdb:
        db.close()
    return toas, row


def get_toas(timfile_id, existdb=None):
    """Get the TOAs for a particular timfile.

        Input:
            timfile_id: The ID of the timfile to get TOAs for.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            toas: A list of TOAs.
    """
    db = existdb or database.Database()
    db.connect()

    select = db.select([db.toas.c.toa_id.distinct(),
                        db.toas.c.process_id,
                        db.toas.c.rawfile_id,
                        db.toas.c.pulsar_id,
                        db.toas.c.obssystem_id,
                        db.toas.c.imjd,
                        db.toas.c.fmjd,
                        (db.toas.c.fmjd+db.toas.c.imjd).label('mjd'),
                        db.toas.c.freq,
                        db.toas.c.toa_unc_us,
                        db.toas.c.bw,
                        db.toas.c.length,
                        db.toas.c.nbin,
                        db.toas.c.goodness_of_fit,
                        db.obssystems.c.name.label('obssystem'),
                        db.obssystems.c.backend,
                        db.obssystems.c.frontend,
                        db.obssystems.c.band_descriptor,
                        db.telescopes.c.telescope_name,
                        db.telescopes.c.telescope_abbrev,
                        db.telescopes.c.telescope_code,
                        db.process.c.version_id,
                        db.process.c.add_time,
                        db.process.c.parfile_id,
                        db.process.c.add_time,
                        db.process.c.manipulator,
                        db.rawfiles.c.filename.label('rawfile'),
                        db.templates.c.filename.label('template'),
                        (db.toas.c.bw/db.rawfiles.c.bw *
                         db.rawfiles.c.nchan).label('nchan')],
                from_obj=[db.toa_tim.\
                    outerjoin(db.toas,
                        onclause=db.toa_tim.c.toa_id ==
                                db.toas.c.toa_id).\
                    join(db.pulsar_aliases,
                        onclause=db.toas.c.pulsar_id ==
                                db.pulsar_aliases.c.pulsar_id).\
                    outerjoin(db.pulsars,
                        onclause=db.pulsar_aliases.c.pulsar_id ==
                                db.pulsars.c.pulsar_id).\
                    outerjoin(db.process,
                        onclause=db.toas.c.process_id ==
                                db.process.c.process_id).\
                    outerjoin(db.rawfiles,
                        onclause=db.rawfiles.c.rawfile_id ==
                                db.toas.c.rawfile_id).\
                    outerjoin(db.templates,
                        onclause=db.templates.c.template_id ==
                                db.toas.c.template_id).\
                    outerjoin(db.obssystems,
                        onclause=db.toas.c.obssystem_id ==
                                db.obssystems.c.obssystem_id).\
                    outerjoin(db.telescopes,
                        onclause=db.telescopes.c.telescope_id ==
                                db.obssystems.c.telescope_id).\
                    join(db.telescope_aliases,
                        onclause=db.telescopes.c.telescope_id ==
                                db.telescope_aliases.c.telescope_id)]).\
                    where(db.toa_tim.c.timfile_id == timfile_id)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()

    if not existdb:
        db.close()
    
    return rows


def write_timfile(toas, timfile, sortkeys=('freq', 'mjd'), flags=(),
                  outname="-", formatter=formatters.tempo2_formatter):
    """Write TOAs to a timfile.
        
        Inputs:
            toas: A list of TOAs.
            timfile: Information about the timfile from the DB.
            flags: A single string containing flags to add to each TOA.
            sortkeys: A list of keys to sort TOAs by.
            outname: The output file's name. (Default: stdout)
            formatter: A formatter function.

        Outputs:
            None
    """
    if outname != '-' and os.path.exists(outname):
        raise errors.FileError("The output timfile sepcified (%s) "
                               "already exists. Doing nothing..." % outname)
    if not timfile['comments']:
        raise errors.BadInputError("Timfile (ID: %d) has no comment!" %
                                   timfile['timfile_id'])

    # Sort TOAs
    utils.sort_by_keys(toas, sortkeys)
    if outname is '-':
        tim = sys.stdout
    else:
        tim = open(outname, 'w')

    wrapper = textwrap.TextWrapper(initial_indent="# ",
                                   subsequent_indent="# ")
    tim.write(wrapper.fill(timfile['comments'])+'\n')
    userinfo = cache.get_userinfo(timfile['user_id'])
    tim.write("# Created by: %s (%s)\n" %
              (userinfo['real_name'], userinfo['email_address']))
    tim.write("#         at: %s\n" % timfile['add_time'])
    tim.write("# Timfile ID: %d\n" % timfile['timfile_id'])
    tim.write("# (Automatically generated by TOASTER)\n")

    lines = formatter(toas, flags)
    tim.write("\n".join(lines)+"\n")
    if outname != '-':
        tim.close()
        notify.print_info("Successfully wrote %d TOAs to timfile (%s)" %
                          (len(toas), outname), 1)


def main(args):
    if args.format not in FORMATTERS:
        raise errors.UnrecognizedValueError("The requested timfile format "
                                            "'%s' is not recognized. "
                                            "Available formats: '%s'." %
                                            (args.format,
                                             "', '".join(sorted(FORMATTERS.keys()))))
    formatter = FORMATTERS[args.format]
    toas, timfile = get_timfile(args.timfile_id)
    # Write TOAs
    write_timfile(toas, timfile, args.sortkeys, args.flags, args.outname,
                  formatter)


if __name__ == '__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)