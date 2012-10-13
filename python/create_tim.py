#!/usr/bin/env python

import sys

import epta_pipeline_utils as epu
import database
import errors

def get_toas(args):
    """Return a dictionary of information for each TOA
        in the DB that matches the search criteria provided.

        Inputs:
            args: Arguments from argparser.

        Output:
            rows: A list of dicts for each matching row.
    """
    db = database.Database()
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
    
    if args.start_mjd is not None:
        whereclause &= ((db.toas.c.imjd+db.toas.c.fmjd) >= args.start_mjd)
    if args.end_mjd is not None:
        whereclause &= ((db.toas.c.imjd+db.toas.c.fmjd) <= args.end_mjd)
    if args.toa_ids:
        whereclause &= (db.toas.c.toa_id.in_(args.toa_ids)) 
    
    select = db.select([db.toas, \
                        db.obssystems.c.name.label('obssystem'), \
                        db.obssystems.c.backend, \
                        db.obssystems.c.frontend, \
                        db.obssystems.c.band_descriptor, \
                        db.telescopes.c.telescope_name, \
                        db.telescopes.c.telescope_abbrev, \
                        db.telescopes.c.telescope_code, \
                        db.process.c.version_id, \
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
                                db.telescope_aliases.c.telescope_id)], \
                distinct=db.toas.c.toa_id).\
                where(whereclause)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()    
    db.close()
    return rows

def main():
    toas = get_toas(args)
    for toa in toas:
        print args.flags % toa
        print '-'*25
    sys.exit(1)

    
    # Open output tim file
    if args.outfile:
         outfile = args.outfile[0]
    else:
         outfile = args.psr[0]+".tim"

    try:
        f_tim = open(outfile,'w')
    except IOError, (errno, strerror):
        print "IOError (%s): %s"%(errno, strerror)

    print "\nFile", outfile, "open.\n"
    f_tim.write("FORMAT 1\n\n")
    
    psr_name = []
    freq = []
    imjd = []
    fmjd = []
    toa_unc_us = []
    obs = []
    rawfilename = []
    for i_row in range(len(DBOUT)):
         psr_name.append(DBOUT[i_row][0])
         obs.append(str(DBOUT[i_row][1]))
         freq.append("%.2lf"%(DBOUT[i_row][2]))
         imjd.append("%5d"%(DBOUT[i_row][3]))
         fmjd.append("%.15lf"%(DBOUT[i_row][4]))
         toa_unc_us.append("%.4lf"%(DBOUT[i_row][5]))
         rawfilename.append(DBOUT[i_row][6])

    # Construct simple TOA lines in accordance with tempo2 format
    for i_row in range(len(DBOUT)):
         cur_line = [" %s "%rawfilename[i_row], \
                     freq[i_row],     \
                     "%s%s  "%(imjd[i_row], fmjd[i_row][1:]), \
                     toa_unc_us[i_row], \
                     obs[i_row]]
         cur_line_str = "  ".join(cur_line)+"\n"
         f_tim.write(cur_line_str)


    # Close profile file
    f_tim.close()


if __name__=='__main__':
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
    parser.add_argument('-o', '--outfn', dest='outfn', 
                        default='output.tim', 
                        help='Name of output file')
    args=parser.parse_args()
    main()
