#!/usr/bin/env python

"""
Show an overview of data reduction.

Patrick Lazarus, Nov 2, 2012
"""

import database
import utils
import errors
import colour
import config

def get_procjobs(args, existdb=None):
    """Return a dictionary of information for each 
        processing job in the DB that matches the
        criteria provided.

        Inputs:
            args: Arguments from argparer.
            existdb: An (optional) existing database connection object.
                (Default: Establish a db connection)
        
        Output:
            rows: A list of dicts for each matching row.
    """
    db = existdb or database.Database()
    db.connect()
    
    whereclause = db.pulsar_aliases.c.pulsar_alias.like(args.pulsar_name)
    if args.manipulators:
        tmp = db.process.c.manipulator.like(args.manipulators[0])
        for manip in args.manipulators[1:]:
            tmp |= (db.process.c.manipulator.like(manip))
        whereclause &= (tmp)
   
    if args.rawfile_ids:
        whereclause &= (db.process.c.rawfile_id.in_(args.rawfile_ids))

    if args.process_ids:
        whereclause &= (db.process.c.process_id.in_(args.process_ids))

    if args.manip_args:
        print args.manip_args
        tmp = db.process.c.manipulator_args.contains(args.manip_args[0])
        for maniparg in args.manip_args[1:]:
            tmp &= (db.process.c.manipulator_args.contains(args.manip_args[0]))
        whereclause &= (tmp)

    select = db.select([db.process.c.process_id.distinct(), \
                        db.process.c.rawfile_id, \
                        db.process.c.template_id, \
                        db.process.c.parfile_id, \
                        db.process.c.user_id, \
                        db.process.c.add_time, \
                        db.process.c.manipulator, \
                        db.process.c.manipulator_args, \
                        db.process.c.nchan, \
                        db.process.c.nsub, \
                        db.process.c.toa_fitting_method, \
                        (db.process.c.nchan*db.process.c.nsub).\
                                label("numtoas"), \
                        db.rawfiles.c.filepath.\
                                label("rawpath"), \
                        db.rawfiles.c.filename.\
                                label("rawfn"), \
                        db.rawfiles.c.pulsar_id, \
                        db.templates.c.filepath.\
                                label("temppath"), \
                        db.templates.c.filename.\
                                label("tempfn"), \
                        db.parfiles.c.filepath.\
                                label("parpath"), \
                        db.parfiles.c.filename.\
                                label("parfn"), \
                        db.users.c.real_name, \
                        db.users.c.email_address, \
                        ], \
                from_obj=[db.process.\
                    outerjoin(db.rawfiles, \
                        onclause=db.rawfiles.c.rawfile_id == \
                                db.process.c.rawfile_id).\
                    join(db.pulsar_aliases, \
                        onclause=db.rawfiles.c.pulsar_id == \
                                db.pulsar_aliases.c.pulsar_id).\
                    outerjoin(db.pulsars, \
                        onclause=db.pulsar_aliases.c.pulsar_id == \
                                db.pulsars.c.pulsar_id).\
                    outerjoin(db.templates, \
                        onclause=db.templates.c.template_id == \
                                db.process.c.template_id).\
                    outerjoin(db.parfiles, \
                        onclause=db.parfiles.c.parfile_id == \
                                db.process.c.parfile_id)]).\
                where(whereclause)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        db.close()
    return rows


def show_procjobs(procjobs):
    print "--"*25
    for procjob in procjobs:
        print colour.cstring("Process Id:", underline=True, bold=True) + \
                colour.cstring(" %d" % procjob.process_id, bold=True)
        print "\nPulsar name: %s" % utils.get_pulsarname(procjob.pulsar_id)
        print "Rawfile (ID=%d): %s" % (procjob.rawfile_id, procjob.rawfn)
        print "Manipulator: %s" % procjob.manipulator
        print "       Args: %s" % procjob.manipulator_args
        print "Number of freq. chunks: %d" % procjob.nchan
        print "Number of time chunks: %d" % procjob.nsub
        print "Uploaded by: %s (%s)" % \
                    (procjob.real_name, procjob.email_address)
        print "Date and time job completed: %s" % procjob.add_time.isoformat(' ')
        if config.cfg.verbosity >= 1:
            lines = ["Template (ID=%d): %s" % \
                            (procjob.template_id, procjob.tempfn), \
                     "Parfile (ID=%d): %s" % \
                            (procjob.parfile_id, procjob.parfn)]
            utils.print_info("\n".join(lines), 1)
        print "--"*25


def summarize_procjobs(procjobs):
    """Print a summary of the processing jobs.

        Input:
            procjobs: A list of row objects, each representing a 
                processing job.

        Output:
            None
    """
    manipulators = {}
    pulsars = {}
    for procjob in procjobs:
        # Manipulators
        nman = manipulators.get(procjob['manipulator'], 0) + 1
        manipulators[procjob['manipulator']] = nman
        # Pulsars
        npsr = pulsars.get(procjob['pulsar_id'], 0) + 1
        pulsars[procjob['pulsar_id']] = npsr
    print "Number of processing jobs: %d" % len(procjobs)
    print "Number of manipulators: %d" % len(manipulators)
    for manip in sorted(manipulators.keys()):
        print "    Number of '%s' processing jobs: %d" % (manip, manipulators[manip])
    print "Number of pulsars: %d" % len(pulsars)


def custom_show_procjobs(procjobs, fmt="%(process_id)d"):
    for procjob in procjobs:
        print fmt.decode('string-escape') % procjob


def main():
    procjobs = get_procjobs(args)
    if not len(procjobs):
        raise errors.ToasterError("No processing jobs match parameters provided!")
    # Sort procjobs
    utils.sort_by_keys(procjobs, args.sortkeys) 
    if args.output_style=='text':
        show_procjobs(procjobs)
    elif args.output_style=='summary':
        summarize_procjobs(procjobs)
    else:
        custom_show_procjobs(procjobs, fmt=args.output_style)


if __name__=='__main__':
    parser = utils.DefaultArguments(description="Get a list of processing " \
                                        "jobs from the DB that match the " \
                                        "given set of criteria.")
    parser.add_argument('-r', '--rawfile-id', dest='rawfile_ids', \
                        type=int, default=[], action='append', \
                        help="A raw file ID. Multiple instances of " \
                            "these criteria may be provided.")
    parser.add_argument('-P', '--process-id', dest='process_ids', \
                        type=int, default=[], action='append', \
                        help="A process ID. Multiple instances of " \
                            "these criteria may be provided.")
    parser.add_argument('-p', '--psr', dest='pulsar_name', \
                        type=str, default='%', \
                        help="The pulsar to grab rawfiles for. " \
                            "NOTE: SQL regular expression syntax may be used")
    parser.add_argument('-m', '--manipulator', dest='manipulators', \
                        type=str, default=[], action='append', \
                        help="The name of the manipulator used to process " \
                            "the data.")
    parser.add_argument('--manipulator-arg', dest='manip_args', \
                        type=str, default=[], action='append', \
                        help="A string contained in the arguments " \
                            "provided to the manipulator. Multiple " \
                            "instances of these criteria may be provided. " \
                            "All such criteria must be provided.")
    parser.add_argument("--output-style", default='text', \
                        dest='output_style', type=str, \
                        help="The following options control how " \
                        "the matching processing jobs are presented. " \
                        "Recognized modes: 'text' - List information. " \
                        "Increase verbosity to get more info; " \
                        "'summary' - Summarize the matching processing " \
                        "jobs. Other styles are python-style format " \
                        "strings interpolated using row-information for " \
                        "each matching rawfile (e.g. 'Manipulator = " \
                        "%%(manipulator)s'). " \
                        "(Default: text).")
    parser.add_argument('--sort', dest='sortkeys', metavar='SORTKEY', \
                        action='append', default=['add_time', 'rawfile_id'], \
                        help="DB column to sort processing jobs by. Multiple " \
                            "--sort options can be provided. Options " \
                            "provided later will take precedence " \
                            "over previous options. (Default: Sort " \
                            "primarily by rawfile_id, then processing " \
                            "date/time)")
    args = parser.parse_args()
    main()
