#!/usr/bin/env python
import datetime
import os.path

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates
import matplotlib.ticker
import matplotlib.patches

import epta_pipeline_utils as epu
import database

# Set default parameters
plt.rc('xtick', labelsize='x-small')
plt.rc('ytick', labelsize='x-small')
plt.rc('axes', labelsize='small')
plt.rc('font', family='sans-serif')

def main():
    db = database.Database()
    db.connect()
    
    try:
        # Make the plot
        make_plot(db)
    finally:
        db.close()

    plt.show()
    #plt.savefig('archived_rawfile_status.png')


def make_plot(existdb=None):
    db = existdb or databse.Database()
    db.connect()
    
    fig = plt.figure(figsize=(10,8))
    titletext = plt.figtext(0.025,0.975, "Raw file Summary", \
                            size='xx-large', ha='left', va='top')
    dbtext = plt.figtext(0.025, 0.025, "Database (%s): %s" % \
                            (db.engine.name, db.engine.url.database), \
                            size='x-small', ha='left', va='bottom')
    timetext = plt.figtext(0.975, 0.025, epu.Give_UTC_now(), \
                            size='x-small', ha='right', va='bottom')
    
    # Get brief summary of all files
    select = db.select([database.sa.func.sum(db.rawfiles.c.filesize).\
                                label('totalbytes'), \
                        database.sa.func.count(db.rawfiles.c.filename).\
                                label('numfiles'), \
                        database.sa.func.sum(db.rawfiles.c.length).\
                                label('totalsecs')])
    result = db.execute(select)
    row = result.fetchone()
    result.close()

    plt.figtext(0.05, 0.91, "Total number of files archived: %d" % row['numfiles'], \
                ha='left', size='medium')
    length = row['totalsecs']
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
    size = row['totalbytes']
    unit = 'bytes'
    other_units = ['TB', 'GB', 'MB', 'KB']
    while size >= 1024.0 and len(other_units) > 1:
        size /= 1024.0
        unit = other_units.pop()
    plt.figtext(0.05, 0.86, "Total disk space used: %.2g %s" % \
                        (size, unit), \
                ha='left', size='medium')

    # Get data
    select = db.select([db.rawfiles.c.add_time, \
                        db.rawfiles.c.mjd, \
                        db.rawfiles.c.length, \
                        db.rawfiles.c.bw, \
                        db.rawfiles.c.freq, \
                        db.rawfiles.c.obssystem_id]).\
                order_by(db.rawfiles.c.add_time.asc())
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    mjds = np.empty(len(rows))
    lengths = np.empty(len(rows))
    bws = np.empty(len(rows))
    freqs = np.empty(len(rows))
    obsids = np.empty(len(rows))
    for ii, row in enumerate(rows):
        length_day = row['length']/86400.0
        mjds[ii] = row['mjd']+length_day/2.0
        lengths[ii] = length_day
        bws[ii] = row['bw']
        freqs[ii] = row['freq']
        obsids[ii] = row['obssystem_id']
    cnorm = matplotlib.colors.Normalize(obsids.min(), obsids.max())
    cmap = plt.get_cmap('gist_rainbow')
    add_times = np.asarray([row['add_time'] for row in rows]+[datetime.datetime.utcnow()])
   
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
    select = db.select([db.telescopes.c.telescope_name, \
                        database.sa.func.count(db.rawfiles.c.filename).\
                                label('count')], \
                from_obj=[db.rawfiles.\
                    outerjoin(db.obssystems, \
                        onclause=db.obssystems.c.obssystem_id == \
                                db.rawfiles.c.obssystem_id).\
                    outerjoin(db.telescopes, \
                        onclause=db.telescopes.c.telescope_id == \
                                db.obssystems.c.telescope_id)]).\
                group_by(db.telescopes.c.telescope_id)

    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    telescopes = []
    counts = []
    for row in rows:
        telescopes.append(row['telescope_name'])
        counts.append(row['count'])
    labels = []
    for t, cnt in zip(telescopes, counts):
        labels.append("%s: %d" % (t, cnt))

    ax = plt.axes((0.35, 0.55, 0.25, 0.25))
    plt.axis('equal')
    #colours = [telescope_colours[tel] for tel in telescopes]
    #tel_pie = plt.pie(counts, labels=labels, colors=colours, autopct='%.1f %%')
    tel_pie = plt.pie(counts, labels=labels, autopct='%.1f %%')
    plt.setp(tel_pie[1]+tel_pie[2], size='xx-small')
    plt.title("Num. raw files by telescope", size='small')

    # Break down by observing band
    select = db.select([db.obssystems.c.band_descriptor, \
                        database.sa.func.count(db.rawfiles.c.filename).\
                                label('count')], \
                from_obj=[db.rawfiles.\
                    outerjoin(db.obssystems, \
                        onclause=db.obssystems.c.obssystem_id == \
                                db.rawfiles.c.obssystem_id)]).\
                group_by(db.obssystems.c.band_descriptor)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    bands = []
    counts = []
    for row in rows:
        bands.append(row['band_descriptor'])
        counts.append(row['count'])
    labels = []
    for t, cnt in zip(bands, counts):
        labels.append("%s: %d" % (t, cnt))

    ax = plt.axes((0.05, 0.55, 0.25, 0.25))
    plt.axis('equal')
    #colours = [telescope_colours[tel] for tel in telescopes]
    #tel_pie = plt.pie(counts, labels=labels, colors=colours, autopct='%.1f %%')
    band_pie = plt.pie(counts, labels=labels, autopct='%.1f %%')
    plt.setp(band_pie[1]+band_pie[2], size='xx-small')
    plt.title("Num. raw files by observing band", size='small')



    # Break down by pulsar
    select = db.select([db.pulsars.c.pulsar_name, \
                        database.sa.func.count(db.rawfiles.c.filename).\
                                label('count'), \
                        database.sa.func.sum(db.rawfiles.c.length).\
                                label('totalsecs')], \
                from_obj=[db.rawfiles.\
                    outerjoin(db.pulsars, \
                        onclause=db.pulsars.c.pulsar_id == \
                                db.rawfiles.c.pulsar_id)]).\
                group_by(db.pulsars.c.pulsar_id)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    
    psrs = []
    counts = []
    hours = []
    for row in rows:
        psrs.append(row['pulsar_name'])
        counts.append(row['count'])
        hours.append(row['totalsecs']/3600.0)
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

    if not existdb:
        db.close()


if __name__=='__main__':
    parser = epu.DefaultArguments(description="Make a plot summarising " \
                                        "data in the archive.")
    args = parser.parse_args()
    main()
