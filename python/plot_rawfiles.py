import datetime
import os.path

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates

import epta_pipeline_utils
import MySQLdb


def main():
    # Get data
    query = "SELECT add_time FROM rawfiles ORDER BY add_time ASC"
    cursor.execute(query)
    add_times = np.asarray([row[0] for row in cursor.fetchall()])
    
    fig = plt.figure(figsize=(10,8))
    ax = plt.axes((0.1, 0.15, 0.85, 0.35))
    times = np.concatenate((np.repeat(add_times, 2), [datetime.datetime.now()]))
    num = np.concatenate(([0], np.repeat(np.arange(1, len(add_times)+1), 2)))
    
    plt.plot(times, num, 'k-')
    plt.xlabel("Add date", size='x-small')
    plt.ylabel("Number of files archived", size='x-small')
    ax.fmt_xdata = matplotlib.dates.DateFormatter("%Y-%m-%d %H:%M")
    fig.autofmt_xdate()
    loc = matplotlib.dates.AutoDateLocator(minticks=4)
    fmt = matplotlib.dates.AutoDateFormatter(loc)
    fmt.scaled[1./24.] = '%a, %I:%M%p'
    ax.xaxis.set_major_locator(loc)
    ax.xaxis.set_major_formatter(fmt)
    plt.ylim(0, len(add_times)*1.1)
    plt.setp(ax.get_xticklabels(), size='xx-small')
    plt.setp(ax.get_yticklabels(), size='xx-small')
    plt.title("Total number of raw files archived", size='small')

    # Make pie charts
    # Break down by telescope
    query = "SELECT t.name, COUNT(*) " \
            "FROM rawfiles AS r " \
            "LEFT JOIN obssystems AS o " \
                "ON o.obssystem_id=r.obssystem_id " \
            "LEFT JOIN telescopes AS t " \
                "ON o.telescope_id=t.telescope_id " \
            "GROUP BY t.name"
    cursor.execute(query)
    telescopes, counts = zip(*cursor.fetchall())
    labels = []
    for t, cnt in zip(telescopes, counts):
        labels.append("%s: %d" % (t, cnt))

    ax = plt.axes((0.3, 0.55, 0.3, 0.3))
    plt.axis('equal')
    tel_pie = plt.pie(counts, labels=labels, autopct='%.1f %%')
    plt.setp(tel_pie[1]+tel_pie[2], size='xx-small')
    plt.title("Num. raw files by telescope", size='small')

    # Break down by pulsar
    query = "SELECT p.pulsar_name, COUNT(*) " \
            "FROM rawfiles AS r " \
            "LEFT JOIN pulsars AS p " \
                "ON r.pulsar_id=p.pulsar_id " \
            "GROUP BY p.pulsar_name"
    cursor.execute(query)
    psrs, counts = zip(*cursor.fetchall())
    labels = []
    for p, cnt in zip(psrs, counts):
        labels.append("%s: %d" % (p, cnt))

    ax = plt.axes((0.65, 0.55, 0.3, 0.3))
    plt.axis('equal')
    psr_pie = plt.pie(counts, labels=labels, autopct='%.1f %%')
    plt.setp(psr_pie[1]+psr_pie[2], size='xx-small')
    plt.title("Num. raw files by pulsar", size='small')
   
    # Get file sizes
    query = "SELECT filepath, filename, length FROM rawfiles"
    cursor.execute(query)
    rawfiles = cursor.fetchall()
    fns = [os.path.join(path, fn) for (path, fn, length) in rawfiles]
    numgb = sum([os.path.getsize(fn) for fn in fns])/(1024.0**3)
    length = sum([row[2] for row in rawfiles])
    plt.figtext(0.05, 0.75, "Total number of files archived: %d" % len(fns), \
                ha='left', size='x-small')
    plt.figtext(0.05, 0.73, "Total disk space used: %.2g GB" % numgb, \
                ha='left', size='x-small')
    plt.figtext(0.05, 0.71, "Total integration time: %.2g hr" % (length/3600), \
                ha='left', size='x-small')
    plt.savefig('archived_rawfile_status.png')


if __name__=='__main__':
    cursor, conn = epta_pipeline_utils.DBconnect()
    try:
        main()
    finally:
        conn.close()

