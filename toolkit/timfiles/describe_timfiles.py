#!/usr/bin/env python

"""Show an overview of timfile entries in the DB.
"""

from toaster import database
from toaster import utils
from toaster import colour
from toaster import errors
from toaster.utils import notify
from toaster.utils import cache
import numpy as np


SHORTNAME = 'show'
DESCRIPTION = "Print an overview of info about timfiles."


def add_arguments(parser):
    parser.add_argument('-p', '--psr', dest='pulsar_name',
                        type=str, default='%',
                        help="The pulsar to grab timfiles for. "
                             "NOTE: SQL regular expression syntax may be used")
    parser.add_argument('--timfile-id', dest='timfile_id',
                        type=int, default=None,
                        help="A timfile ID. This is useful for checking "
                             "the details of a single timfile, identified "
                             "by its ID number. NOTE: No other timfiles "
                             "will match if this option is provided.")
    parser.add_argument("-O", "--output-style", default='text',
                        dest='output_style', type=str,
                        help="The following options control how "
                             "the matching processing jobs are presented. "
                             "Recognized modes: 'text' - List information. "
                             "Increase verbosity to get more info; 'plot' - "
                             "Show a plot summarizing all TOAs in a timfile "
                             "NOTE: This only applies when a single timfile "
                             "matches the criteria provided. (Default: text).")


def get_timfiles_toas(timfile_id):
    """Return TOA information for each of the given timfile's
        TOAs.

        Input:
            timfile_id: The ID number of the timfile to get
                TOAs for.

        Output:
            toas: A list of row objects, each corresponding to
                a TOA of the timfile.
    """
    db = database.Database()
    db.connect()

    select = db.select([db.toas.c.toa_id,
                        db.toas.c.toa_unc_us,
                        (db.toas.c.fmjd+db.toas.c.imjd).\
                                label('mjd'),
                        db.replacement_rawfiles.c.replacement_rawfile_id,
                        db.obssystems.c.band_descriptor,
                        db.obssystems.c.name,
                        db.obssystems.c.obssystem_id],
                from_obj=[db.toa_tim.\
                    outerjoin(db.toas,
                        onclause=db.toas.c.toa_id ==
                                db.toa_tim.c.toa_id).\
                    outerjoin(db.replacement_rawfiles,
                        onclause=db.toas.c.rawfile_id ==
                                db.replacement_rawfiles.c.obsolete_rawfile_id).\
                    outerjoin(db.obssystems,
                        onclause=db.toas.c.obssystem_id ==
                                db.obssystems.c.obssystem_id)]).\
                where(db.toa_tim.c.timfile_id == timfile_id)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    db.close()
    return rows

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
        whereclause &= (db.timfiles.c.timfile_id == timfile_id)

    select = db.select([db.timfiles,
                        db.users.c.real_name,
                        db.users.c.email_address,
                        db.pulsars.c.pulsar_name,
                        db.master_timfiles.c.timfile_id.label('mtimid'),
                        database.sa.func.count(db.toa_tim.c.toa_id.distinct()).\
                                    label('numtoas'),
                        database.sa.func.max(db.toas.c.fmjd+db.toas.c.imjd).\
                                    label('endmjd'),
                        database.sa.func.min(db.toas.c.fmjd+db.toas.c.imjd).\
                                    label('startmjd'),
                        database.sa.func.count(db.obssystems.c.telescope_id.distinct()).\
                                    label('numtelescopes'),
                        database.sa.func.count(db.toas.c.obssystem_id.distinct()).\
                                    label('numobsys'),
                        database.sa.func.max(db.replacement_rawfiles.c.replacement_rawfile_id).\
                                    label('any_replaced')],
                from_obj=[db.timfiles.\
                    join(db.pulsar_aliases,
                        onclause=db.timfiles.c.pulsar_id ==
                                db.pulsar_aliases.c.pulsar_id).\
                    outerjoin(db.pulsars,
                        onclause=db.timfiles.c.pulsar_id ==
                                db.pulsars.c.pulsar_id).\
                    outerjoin(db.master_timfiles,
                        onclause=db.master_timfiles.c.timfile_id ==
                                    db.timfiles.c.timfile_id).\
                    outerjoin(db.users,
                        onclause=db.users.c.user_id ==
                                db.timfiles.c.user_id).\
                    join(db.toa_tim,
                        onclause=db.toa_tim.c.timfile_id ==
                                db.timfiles.c.timfile_id).\
                    outerjoin(db.toas,
                        onclause=db.toa_tim.c.toa_id ==
                                db.toas.c.toa_id).\
                    outerjoin(db.replacement_rawfiles,
                        onclause=db.toas.c.rawfile_id ==
                                db.replacement_rawfiles.c.obsolete_rawfile_id).\
                    outerjoin(db.obssystems,
                        onclause=db.toas.c.obssystem_id ==
                                db.obssystems.c.obssystem_id)],
                distinct=db.timfiles.c.timfile_id).\
                where(whereclause).\
                group_by(db.timfiles.c.timfile_id)
    result = db.execute(select)
    # MySQL return a single row filled with Nones and 0s
    # if no timfiles match. Weed out the bad row.
    rows = [r for r in result.fetchall() if r['timfile_id'] is not None]
    result.close()
    db.close()
    return rows


def show_timfiles(timfiles):
    if not len(timfiles):
        raise errors.ToasterError("No timfiles match parameters provided!")

    print("--"*25)
    for timfile in timfiles:
        print(colour.cstring("Timfile ID:", underline=True, bold=True) + \
            colour.cstring(" %d" % timfile['timfile_id'], bold=True))
        print("Pulsar name: %s" % timfile['pulsar_name'])
        print("Master timfile? %s" % \
                    (((timfile['mtimid'] is not None) and "Yes") or "No"))
        print("Last edited by: %s (%s)" % (timfile['real_name'],
                                            timfile['email_address']))
        print("Comments: %s" % timfile['comments'])
        print("Date and time timfile was last edited: %s" % \
            timfile['add_time'].isoformat(' '))
        print("Number of TOAs: %d" % timfile['numtoas'])
        if timfile['any_replaced'] is not None:
            colour.cprint("Some TOAs are from rawfiles that been "
                            "superseded", 'warning')

        # Show extra information if verbosity is >= 1
        lines = ["First TOA (MJD): %s" % timfile['startmjd'],
                    "Last TOA (MJD): %s" % timfile['endmjd'],
                    "Number of telescopes used: %d" % timfile['numtelescopes'],
                    "Number of observing systems used: %d" % timfile['numobsys']]
        notify.print_info("\n".join(lines), 1)
        print("--"*25)


def plot_timfile(timfile):
    """Make a plot summarizing a timfile.

        Input:
            timfile: A row of info of the timfile to summarize.

        Output:
            None
    """
    import matplotlib.pyplot as plt
    import matplotlib
    
    COLOURS = ['k', 'g', 'r', 'b', 'm', 'c', 'y']
    ncolours = len(COLOURS)
    BANDS = ['UHF', 'L-band', 'S-band']
    numbands = len(BANDS)
    toas = get_timfiles_toas(timfile['timfile_id'])
    obssys_ids = set()
    for toa in toas:
        obssys_ids.add(toa['obssystem_id'])
    obssys_ids = list(obssys_ids)
    fig = plt.figure()
    ax = plt.axes((0.1, 0.15, 0.85, 0.75))
    lomjd = 70000
    himjd = 10000
    artists = []
    for toa in toas:
        ind = BANDS.index(toa['band_descriptor'])
        ymin = float(ind)/numbands
        ymax = float(ind+1)/numbands
        cc = COLOURS[obssys_ids.index(toa['obssystem_id']) % ncolours]
        artists.append(plt.axvline(toa['mjd'], ymin, ymax, c=cc))
        himjd = max(himjd, toa['mjd'])
        lomjd = min(lomjd, toa['mjd'])
    plt.xlabel("MJD")
    plt.yticks(np.arange(0.5/numbands, 1, 1.0/numbands), BANDS,
               rotation=30, va='top')
    plt.xlim(lomjd, himjd)
    patches = []
    obssystems = []
    for ii, obssys_id in enumerate(obssys_ids):
        cc = COLOURS[ii % ncolours]
        patches.append(matplotlib.patches.Patch(fc=cc))
        obssystems.append(cache.get_obssysinfo(obssys_id)['name'])
    plt.figlegend(patches, obssystems, 'lower center', ncol=4,
                  prop=dict(size='small'))

    def change_thickness(event):
        if event.key == '=':
            for art in artists:
                lw = art.get_linewidth()
                art.set_linewidth(lw+0.5)
        elif event.key == '-':
            for art in artists:
                lw = art.get_linewidth()
                art.set_linewidth(max(1, lw-0.5))
        plt.draw()
    fig.canvas.mpl_connect('key_press_event', change_thickness)


def main(args):
    timfiles = get_timfiles(args.pulsar_name, args.timfile_id)
    if args.output_style == 'text':
        show_timfiles(timfiles)
    elif args.output_style == 'plot':
        if len(timfiles) == 1:
            import matplotlib.pyplot as plt
            plot_timfile(timfiles[0])
            plt.show()
        else:
            raise errors.BadInputError("Timfile summary plot only applies "
                                       "when a single timfile matches the "
                                       "criteria provided. (%d matches)" %
                                       len(timfiles))
    else:
        raise errors.UnrecognizedValueError("The output-style (%s) isn't "
                                            "recognized." % args.output_style)


if __name__ == '__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
