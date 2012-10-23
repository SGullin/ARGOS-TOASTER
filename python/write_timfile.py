#!/usr/bin/env python
"""Write a timfile from an entry in the database.
"""
import os.path
import sys
import textwrap
import types

import database
import errors
import epta_pipeline_utils as epu

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
                where(db.timfiles.c.timfile_id==timfile_id)
    result = db.execute(select)
    row = result.fetchone()
    result.close()

    if not row:
        raise errors.DatabaseError("There is no timfile with ID=%d" % \
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
                        db.telescopes.c.telescope_name, \
                        db.telescopes.c.telescope_abbrev, \
                        db.telescopes.c.telescope_code, \
                        db.process.c.version_id, \
                        db.process.c.add_time, \
                        db.rawfiles.c.filename.label('rawfile'), \
                        db.templates.c.filename.label('template'), \
                        (db.toas.c.bw/db.rawfiles.c.bw * \
                                db.rawfiles.c.nchan).label('nchan')], \
                from_obj=[db.toa_tim.\
                    outerjoin(db.toas, \
                        onclause=db.toa_tim.c.toa_id == \
                                db.toas.c.toa_id).\
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
                    where(db.toa_tim.c.timfile_id==timfile_id)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()

    if not existdb:
        db.close()
    
    return rows


def write_timfile(toas, timfile, sortkeys=['freq', 'mjd'], flags="", outname="-"):
    """Write TOAs to a timfile.
        
        Inputs:
            toas: A list of TOAs.
            timfile: Information about the timfile from the DB.
            flags: A list of flags to add to each TOA.
            sortkeys: A list of keys to sort TOAs by.
            comments: A user comment describing the timfile.
            outname: The output file's name.

        Outputs:
            None
    """
    if os.path.exists(outname):
        raise errors.FileError("The output timfile sepcified (%s) " \
                                "already exists. Doing nothing...")
    
    # Sort TOAs
    for sortkey in sortkeys:
        if sortkey.endswith("_r"):
            sortkey = sortkey[:-2]
            rev = True
            epu.print_info("Reverse sorting TOAs by %s..." % sortkey, 2)
        else:
            rev = False
            epu.print_info("Sorting TOAs by %s..." % sortkey, 2)
        if sortkey not in toas[0].keys():
            raise errors.UnrecognizedValueError("The sorting key (%s) " \
                                    "is not recognized." % sortkey)
        if type(toas[0][sortkey]) is types.StringType:
            toas.sort(key=lambda x: x[sortkey].lower(), reverse=rev)
        else:
            toas.sort(key=lambda x: x[sortkey], reverse=rev)

    if outname is '-':
        tim = sys.stdout
    else:
        tim = open(outname, 'w')

    wrapper = textwrap.TextWrapper(initial_indent="# ", \
                                   subsequent_indent="# ")
    tim.write(wrapper.fill(timfile['comments'])+'\n')
    # tim.write("# Created by: %s (%s)\n" % NotImplemented)
    userinfo = epu.get_userinfo(timfile['user_id']) 
    tim.write("# Created by: %s (%s)\n" % \
                    (userinfo['real_name'], userinfo['email_address']))
    tim.write("#         at: %s\n" % timfile['add_time'])
    tim.write("# (Automatically generated by TOASTER)\n")
    
    tim.write("FORMAT 1\n") # TEMPO2 format
    for toa in toas:
        fmjdstr = str(toa['fmjd'])
        mjd = "%s%s" % (toa['imjd'], fmjdstr[fmjdstr.index('.'):])
        toastr = "%s %.3f %s %.3f %s" % \
                    (toa['rawfile'], toa['freq'], mjd, \
                        toa['toa_unc_us'], toa['telescope_code'])
        flagstr = flags % toa
        tim.write("%s %s\n" % (toastr, flagstr))

    if outname != '-':
        tim.close()
        epu.print_info("Successfully wrote %d TOAs to timfile (%s)" % \
                        (len(toas), outname), 1)


def main():
    toas, timfile = get_timfile(args.timfile_id)
    # Write TOAs
    write_timfile(toas, timfile, args.sortkeys, args.flags, args.outname)


if __name__=='__main__':
    parser = epu.DefaultArguments(description='Writes out a tim file ' \
                                'already defined in the DB. The output ' \
                                'format is suitable for TEMPO2.')
    parser.add_argument('-t', '--timfile-id', dest='timfile_id', \
                        required=True, type=int, \
                        help="The ID of the timefile entry in the DB to " \
                            "write out. NOTE: This is required.")
    parser.add_argument('-o', '--outname', dest='outname', \
                        default='-', type=str, \
                        help="Output timfile's name. NOTE: This is "
                            "required.")
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
    parser.add_argument('--sort', dest='sortkeys', \
        action='append', default=[], \
        help="DB column to sort TOAs by. Multiple --sort options can " \
            "be provided. Options provided later will take precedent " \
            "over previous options. (Default: Sort by MJD, then freq.)")
    args=parser.parse_args()
    main()
