#!/usr/bin/env python

"""Show an overview of timfile entries in the DB.
"""

import database
import epta_pipeline_utils as epu
import colour

def get_timfiles(psr='%', timfile_id=None):
    """Return a dictionary of information for each timfile
        in the DB that matches the search criteria provided.

        Inputs:
            psr: A SQL-style regular expression to match with
                pulsar J- and B-names.
            timfile_id: The ID number of a specific timfile.

        Output:
            rows: A list of dicts for each matching timfile.
    """
    db = database.Database()
    db.connect()

    whereclause = db.pulsar_aliases.c.pulsar_alias.like(psr)
    if timfile_id is not None:
        whereclause &= (db.timfiles.c.timfile_id==timfile_id)

    select = db.select([db.timfiles, \
                        db.users.c.real_name, \
                        db.users.c.email_address, \
                        db.pulsars.c.pulsar_name, \
                        database.sa.func.count(db.toa_tim.c.toa_id.distinct()).\
                                    label('numtoas'), \
                        database.sa.func.max(db.toas.c.fmjd+db.toas.c.imjd).\
                                    label('endmjd'), \
                        database.sa.func.min(db.toas.c.fmjd+db.toas.c.imjd).\
                                    label('startmjd'), \
                        database.sa.func.count(db.obssystems.c.telescope_id.distinct()).\
                                    label('numtelescopes'), \
                        database.sa.func.count(db.toas.c.obssystem_id.distinct()).\
                                    label('numobsys')], \
                from_obj=[db.timfiles.\
                    join(db.pulsar_aliases, \
                        onclause=db.timfiles.c.pulsar_id == \
                                db.pulsar_aliases.c.pulsar_id).\
                    outerjoin(db.pulsars, \
                        onclause=db.timfiles.c.pulsar_id == \
                                db.pulsars.c.pulsar_id).\
                    outerjoin(db.users, \
                        onclause=db.users.c.user_id == \
                                db.timfiles.c.user_id).\
                    join(db.toa_tim, \
                        onclause=db.toa_tim.c.timfile_id == \
                                db.timfiles.c.timfile_id).\
                    outerjoin(db.toas, \
                        onclause=db.toa_tim.c.toa_id == \
                                db.toas.c.toa_id).\
                    outerjoin(db.obssystems, \
                        onclause=db.toas.c.obssystem_id == \
                                db.obssystems.c.obssystem_id)], \
                distinct=db.timfiles.c.timfile_id).\
                where(whereclause)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    db.close()
    return rows


def show_timfiles(timfiles):
    if len(timfiles):
        for timfile in timfiles:
            print "- "*25
            print colour.cstring("Timfile ID:", underline=True, bold=True) + \
                    colour.cstring(" %d" % timfile['timfile_id'], bold=True)
            print "Pulsar name: %s" % timfile['pulsar_name']
            print "Uploaded by: %s (%s)" % (timfile['real_name'], \
                                            timfile['email_address'])
            print "Uploader's comments: %s" % timfile['comments']
            print "Date and time timfile was created: %s" % \
                        timfile['add_time'].isoformat(' ')
            print "Number of TOAs: %d" % timfile['numtoas']

            # Show extra information if verbosity is >= 1
            lines = ["First TOA (MJD): %s" % timfile['startmjd'], \
                     "Last TOA (MJD): %s" % timfile['endmjd'], \
                     "Number of telescopes used: %d" % timfile['numtelescopes'], \
                     "Number of observing systems used: %d" % timfile['numobsys']]
            epu.print_info("\n".join(lines), 1)
            print " -"*25
    else:
        raise errors.EptaPipelineError("No timfiles match parameters provided!")

            

def main():
    timfiles = get_timfiles(args.pulsar_name, args.timfile_id)
    show_timfiles(timfiles)


if __name__=='__main__':
    parser = epu.DefaultArguments(description="Print an overview of info " \
                                            "about timfiles.")
    parser.add_argument('-p', '--psr', dest='pulsar_name', \
                        type=str, default='%', \
                        help="The pulsar to grab timfiles for. " \
                            "NOTE: SQL regular expression syntax may be used")
    parser.add_argument('--timfile-id', dest='timfile_id', \
                        type=int, default=None, \
                        help="A timfile ID. This is useful for checking " \
                            "the details of a single timfile, identified " \
                            "by its ID number. NOTE: No other timfiles " \
                            "will match if this option is provided.")
    args = parser.parse_args()
    main()
