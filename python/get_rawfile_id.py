#!/usr/bin/env python

"""
This TOASTER utility script provides the user with a listing
of rawfile_id values from the database to help the user choose which
input is most appropriate.

Patrick Lazarus, Jan. 8, 2012.
"""
import datetime
import os.path
import warnings

import numpy as np

import utils
import database
import errors
import colour
import config

def main():
    rawfiles = get_rawfiles(args)
    if not len(rawfiles):
        raise errors.ToasterError("No rawfiles match parameters provided!")
    if args.output_style=='text':
        show_rawfiles(rawfiles)
    elif args.output_style=='plot':
        plot_rawfiles(rawfiles)
        plt.show()
    elif args.output_style=='summary':
        summarize_rawfiles(rawfiles)
    else:
        custom_show_rawfiles(rawfiles, fmt=args.output_style)


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
        whereclause &= (db.telescope_aliases.c.telescope_alias.\
                                like(args.telescope))
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
                        db.rawfiles.c.filesize, \
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
                        db.obssystems.c.name.label('obssystem'), \
                        db.obssystems.c.frontend, \
                        db.obssystems.c.backend, \
                        db.obssystems.c.band_descriptor, \
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


def custom_show_rawfiles(rawfiles, fmt="%(rawfile_id)d"):
    for rawfile in rawfiles:
        print fmt.decode('string-escape') % rawfile


def summarize_rawfiles(rawfiles):
    numfiles = 0
    size = 0
    length = 0
    for ii, rawfile in enumerate(rawfiles):
        numfiles += 1
        size += rawfile['filesize']
        length += rawfile['length']
    print "Total number of matching raw files in archive: %d" % numfiles
    unit = 's'
    thresh = 60.0
    other_thresh = [365.0, 24.0, 60.0]
    other_units = ['years', 'days', 'hr', 'min']
    while length >= thresh and len(other_units) > 1:
        length /= thresh
        thresh = other_thresh.pop()
        unit = other_units.pop()
    print "Total integration time: %.2g %s" % (length, unit)
    unit = 'bytes'
    other_units = ['TB', 'GB', 'MB', 'KB']
    while size >= 1024.0 and len(other_units) > 1:
        size /= 1024.0
        unit = other_units.pop()
    print "Total disk space used: %.2f %s" % (size, unit)


def plot_rawfiles(rawfiles):
    import matplotlib.pyplot as plt
    import matplotlib

    # Set default parameters
    plt.rc('xtick', labelsize='x-small')
    plt.rc('ytick', labelsize='x-small')
    plt.rc('axes', labelsize='small')
    plt.rc('font', family='sans-serif')

    fig = plt.figure(figsize=(10,8))
    titletext = plt.figtext(0.025,0.975, "Raw file Summary", \
                            size='xx-large', ha='left', va='top')
    db = database.Database() # Get database info, but don't connect
    dbtext = plt.figtext(0.025, 0.025, "Database (%s): %s" % \
                            (db.engine.name, db.engine.url.database), \
                            size='x-small', ha='left', va='bottom')
    timetext = plt.figtext(0.975, 0.025, utils.Give_UTC_now(), \
                            size='x-small', ha='right', va='bottom')

    # Compute data for plotting
    numfiles = 0
    size = 0
    length = 0
    mjds = np.empty(len(rawfiles))
    lengths = np.empty(len(rawfiles))
    bws = np.empty(len(rawfiles))
    freqs = np.empty(len(rawfiles))
    obsids = np.empty(len(rawfiles))
    add_times = []
    telescopes = {}
    band_descriptors = {}
    pulsars = {}
    for ii, rawfile in enumerate(rawfiles):
        numfiles += 1
        size += rawfile['filesize']
        secs = rawfile['length']
        length += secs
        length_day = secs/86400.0
        mjds[ii] = rawfile['mjd']+length_day/2.0
        lengths[ii] = length_day
        bws[ii] = rawfile['bw']
        freqs[ii] = rawfile['freq']
        obsids[ii] = rawfile['obssystem_id']
        add_times.append(rawfile['add_time'])
        tname = rawfile['telescope_name']
        telescopes[tname] = telescopes.get(tname, 0) + 1
        band = rawfile['band_descriptor']
        band_descriptors[band] = band_descriptors.get(band, 0) + 1
        psr = rawfile['pulsar_name']
        psrcnt, psrhr = pulsars.get(psr, (0, 0))
        pulsars[psr] = (psrcnt+1, psrhr+secs/3600.0)
    add_times = np.asarray(sorted(add_times+[datetime.datetime.utcnow()]))

    plt.figtext(0.05, 0.91, "Total number of files archived: %d" % numfiles, \
                ha='left', size='medium')
    unit = 's'
    thresh = 60.0
    other_thresh = [365.0, 24.0, 60.0]
    other_units = ['years', 'days', 'hr', 'min']
    while length >= thresh and len(other_units) > 1:
        length /= thresh
        thresh = other_thresh.pop()
        unit = other_units.pop()
    plt.figtext(0.05, 0.885, "Total integration time: %.2g %s" % \
                        (length, unit), \
                ha='left', size='medium')
    unit = 'bytes'
    other_units = ['TB', 'GB', 'MB', 'KB']
    while size >= 1024.0 and len(other_units) > 1:
        size /= 1024.0
        unit = other_units.pop()
    plt.figtext(0.05, 0.86, "Total disk space used: %.2f %s" % \
                        (size, unit), \
                ha='left', size='medium')

    #cnorm = matplotlib.colors.Normalize(obsids.min(), obsids.max())
    #cmap = plt.get_cmap('gist_rainbow')
   
    ax = plt.axes((0.1, 0.375, 0.45, 0.15))
    #plt.errorbar(mjds, freqs, xerr=lengths/2.0, yerr=bws/2.0, \
    #                ls='None', ecolor='k')
    #for ii in xrange(len(rows)):
    #    ellipse = matplotlib.patches.Ellipse((mjds[ii], freqs[ii]), \
    #                    width=lengths[ii], height=bws[ii], \
    #                    ec='none', fc=cmap(cnorm(obsids[ii])), \
    #                    alpha=0.9)
    #    ax.add_patch(ellipse)
    plt.scatter(mjds, freqs, marker='o', alpha=0.7, c=obsids)
    mjd_range = mjds.ptp()
    plt.xlim(mjds.min()-0.1*mjd_range, mjds.max()+0.1*mjd_range)
    freq_range = freqs.ptp()
    plt.ylim(freqs.min()-0.1*freq_range, freqs.max()+0.1*freq_range)
    plt.xlabel("MJD")
    plt.ylabel("Freq (MHz)")
    fmt = matplotlib.ticker.ScalarFormatter(useOffset=False)
    fmt.set_scientific(False)
    ax.xaxis.set_major_formatter(fmt)
    ax.yaxis.set_major_formatter(fmt)

    ax = plt.axes((0.1, 0.15, 0.45, 0.15))
    plt.plot(add_times, np.arange(len(add_times)), 'k-', drawstyle='steps')
    plt.xlabel("Add date")
    plt.ylabel("Num. files\narchived")
    ax.fmt_xdata = matplotlib.dates.DateFormatter("%Y-%m-%d %H:%M")
    fig.autofmt_xdate()
    loc = matplotlib.dates.AutoDateLocator()
    fmt = matplotlib.dates.AutoDateFormatter(loc)
    #fmt.scaled[1./24.] = '%a, %I:%M%p'
    ax.xaxis.set_major_locator(loc)
    ax.xaxis.set_major_formatter(fmt)
    plt.ylim(0, len(add_times)*1.1)
    plt.title("Total number of raw files archived", size='small')
    plt.xticks(rotation=30, ha='right')
    # Make pie charts
    # Break down by telescope
    tnames = []
    labels = []
    counts = []
    for t, cnt in telescopes.iteritems():
        labels.append("%s: %d" % (t, cnt))
        tnames.append(t)
        counts.append(cnt)

    ax = plt.axes((0.35, 0.55, 0.25, 0.25))
    plt.axis('equal')
    #tel_pie = plt.pie(counts, labels=labels, colors=colours, autopct='%.1f %%')
    tel_pie = plt.pie(counts, labels=labels, autopct='%.1f %%')
    plt.setp(tel_pie[1]+tel_pie[2], size='xx-small')
    plt.title("Num. raw files by telescope", size='small')

    # Break down by observing band
    bands = []
    counts = []
    labels = []
    for b, cnt in band_descriptors.iteritems():
        bands.append(b)
        counts.append(cnt)
        labels.append("%s: %d" % (b, cnt))

    ax = plt.axes((0.05, 0.55, 0.25, 0.25))
    plt.axis('equal')
    #tel_pie = plt.pie(counts, labels=labels, colors=colours, autopct='%.1f %%')
    band_pie = plt.pie(counts, labels=labels, autopct='%.1f %%')
    plt.setp(band_pie[1]+band_pie[2], size='xx-small')
    plt.title("Num. raw files by observing band", size='small')
    
    psrs = []
    counts = []
    hours = []
    for p, (cnt, hr) in pulsars.iteritems():
        psrs.append(p)
        counts.append(cnt)
        hours.append(hr)
    ipsr = np.arange(len(psrs))

    psrtime_ax = plt.axes((0.83, 0.15, 0.12, 0.7))
    psrtime_bar = plt.barh(ipsr, hours, \
                    align='center', lw=0, fc='#B22222', \
                    alpha=0.7, ec='k')
    plt.xlim(0, np.max(hours)*1.1)
    plt.xlabel("Hours")
    plt.setp(psrtime_ax.yaxis.get_ticklabels(), visible=False)
    plt.title("Obs. time", size='small') 
    
    psrcnt_ax = plt.axes((0.7, 0.15, 0.12, 0.7), sharey=psrtime_ax)
    psrcnt_bar = plt.barh(ipsr, counts, \
                    align='center', lw=0, fc='#008080', \
                    alpha=0.7, ec='k')
    plt.xlim(0, np.max(counts)*1.1)
    plt.ylim(-0.5,len(psrs)-0.5)
    plt.yticks(ipsr, psrs, rotation=30, \
                    va='top', ha='right')
    plt.title("# of archives", size='small') 


def show_rawfiles(rawfiles):
    print "--"*25
    for rawdict in rawfiles:
        print colour.cstring("Rawfile ID:", underline=True, bold=True) + \
                colour.cstring(" %d" % rawdict.rawfile_id, bold=True)
        fn = os.path.join(rawdict.filepath, rawdict.filename)
        print "\nRawfile: %s" % fn
        print "Pulsar name: %s" % rawdict.pulsar_name
        print "Uploaded by: %s (%s)" % \
                    (rawdict.real_name, rawdict.email_address)
        print "Date and time rawfile was added: %s" % rawdict.add_time.isoformat(' ')
        if config.cfg.verbosity >= 1:
            lines = ["Observing system ID: %d" % rawdict.obssystem_id, \
                     "Observing system name: %s" % rawdict.obssystem, \
                     "Observing band: %s" % rawdict.band_descriptor, \
                     "Telescope: %s" % rawdict.telescope_name, \
                     "Frontend: %s" % rawdict.frontend, \
                     "Backend: %s" % rawdict.backend, \
                     "Clock: %s" % rawdict.clock]
            utils.print_info("\n".join(lines), 1)
        if config.cfg.verbosity >= 2:
            lines = ["MJD: %.6f" % rawdict.mjd, \
                     "Number of phase bins: %d" % rawdict.nbin, \
                     "Number of channels: %d" % rawdict.nchan, \
                     "Number of polarisations: %d" % rawdict.npol, \
                     "Number of sub-integrations: %d" % rawdict.nsub, \
                     "Centre frequency (MHz): %g" % rawdict.freq, \
                     "Bandwidth (MHz): %g" % rawdict.bw, \
                     "Dispersion measure (pc cm^-3): %g" % rawdict.dm, \
                     "Integration time (s): %g" % rawdict.length]
            utils.print_info("\n".join(lines), 2)
        if config.cfg.verbosity >= 3:
            # Get diagnostics
            db = database.Database()
            db.connect()
            select = db.select([db.raw_diagnostics.c.type, \
                                db.raw_diagnostics.c.value]).\
                        where(db.raw_diagnostics.c.rawfile_id == \
                                    rawdict.rawfile_id)
            result = db.execute(select)
            diags = result.fetchall()
            result.close()
            select = db.select([db.raw_diagnostic_plots.c.plot_type, \
                                db.raw_diagnostic_plots.c.filepath, \
                                db.raw_diagnostic_plots.c.filename]).\
                        where(db.raw_diagnostic_plots.c.rawfile_id == \
                                    rawdict.rawfile_id)
            result = db.execute(select)
            diag_plots = result.fetchall()
            result.close()
            db.close()
            lines = []
            if diags:
                lines.append("Diagnostics:")
                for diag in diags:
                    lines.append("    %s: %g" % (diag['type'], diag['value']))
            if diag_plots:
                lines.append("Diagnostic plots:")
                for diag_plot in diag_plots:
                    lines.append("    %s: %s" % (diag_plot['plot_type'], \
                                os.path.join(diag_plot['filepath'], diag_plot['filename'])))
            utils.print_info("\n".join(lines), 3)
        print "--"*25


if __name__=='__main__':
    parser = utils.DefaultArguments(description="Get a listing of rawfile_id " \
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
    parser.add_argument("--output-style", default='text', \
                        dest='output_style', type=str, \
                        help="The following options control how " \
                        "the matching rawfiles are presented. Recognized " \
                        "modes: 'text' - List information. Increase " \
                        "verbosity to get more info; 'plot' - display " \
                        "a plot; 'summary' - Provide a short summary " \
                        "of the matching rawfiles. Other styles are " \
                        "python-style format " \
                        "strings interpolated using row-information for " \
                        "each matching rawfile (e.g. 'MJD %%(mjd)d'). " \
                        "(Default: text).")
    args = parser.parse_args()
    main()
