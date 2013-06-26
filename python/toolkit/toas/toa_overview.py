#!/usr/bin/env python

import numpy as np

import utils
import errors
import database

from toolkit.timfiles import create_timfile

SHORTNAME = "overview"
DESCRIPTION = "Provide an overview of TOAs"


def add_arguments(parser):
    parser.add_argument("-O", "--output-style", default='histogram', \
                        dest='output_style', type=str, \
                        help="The following options control how " \
                        "the matching rawfiles are presented. Recognized " \
                        "modes: 'histogram' - display a histogram plots; " \
                        "'cadence' - display a plot of observing cadence." \
                        "(Default: histogram).")
    parser.add_argument('-p', '--psr', dest='pulsar_name', \
                        type=str, default='%', \
                        help='Pulsar name, or alias. NOTE: This option ' \
                            'must be provided.')
    parser.add_argument('-P', '--process-id', dest='process_ids', \
                        type=int, default=[], action='append', \
                        help="A process ID. Multiple instances of " \
                            "these criteria may be provided.")
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
    parser.add_argument('-m', '--manipulator', dest='manipulators', \
                        type=str, default=[], action='append', \
                        help="Name of manipulator to match. Multiple '-m/" \
                            "--manipulator' arguments may be provided. " \
                            "(Default: match all manipulators).")


def plot_cadence(toas):
    """Given a list of TOAs (as returned by create_timfile.get_toas(...)
        make a plot of observing cadence.

        Input:
            toas: A list of TOAs.

        Output:
            fig: The newly created matplotlib Figure object.
    """
    import matplotlib.pyplot as plt
    fig = plt.figure(figsize=(10,6))

    # Summarize TOA info
    pulsars = {}
    for toa in toas:
        psr = pulsars.setdefault(toa['pulsar_id'], [])
        psr.append(toa['mjd'])
    indices = []
    labels = []
    ax = plt.axes()
    for ii, (psrid, mjds) in enumerate(pulsars.iteritems()):
        indices.append(ii)
        labels.append(utils.get_pulsarname(psrid))
        ax.plot(mjds, ii*np.ones_like(mjds), 'k.')
    ax.set_xlabel("MJD")
    ax.yaxis.set_ticklabels(labels)
    ax.yaxis.set_ticks(np.array(indices))
    ax.set_ylim(-0.5, len(pulsars)-0.5)
    return fig


def plot_toa_histogram(toas):
    """Given a list of TOAs (as returned by create_timfile.get_toas(...)
        make histogram plots.

        Input:
            toas: A list of TOAs.

        Output:
            fig: The newly created matplotlib Figure object.
    """
    import matplotlib.pyplot as plt
    fig = plt.figure(figsize=(10,6))

    # Summarize TOA info
    pulsars = {}
    telescopes = set()
    bands = set()
    psrnames = set()
    for toa in toas:
        psr = pulsars.setdefault(toa['pulsar_id'], {})
        obssysid = toa['obssystem_id']
        obssysinfo = utils.get_obssysinfo(obssysid)
        telid = obssysinfo['telescope_id']
        telname = utils.get_telescope_info(telid)['telescope_name']
        band = toa['band_descriptor']
        bands.add(band)
        telescopes.add(telname)
        psr[telname] = psr.get(telname, 0)+1
        psr[band] = psr.get(band, 0)+1

    band_colours = dict(zip(sorted(bands), ['r', 'b', 'g', 'c']))
    telescope_colours = {'Effelsberg': '#FFCE00',
                         'Jodrell': '#CE1124', 
                         'Nancay': '#0055A4', 
                         'WSRT': '#FF7F00', 
                         'Sardinia': '#007FFF',
                         'Parkes': '#EDC9AF',
                         'GBT': '#22BB22', \
                         'Arecibo': '#00BFFF'}

    indices = []
    labels = []
    telleg = {}
    bandleg = {}
    telax = plt.axes((0.15, 0.1, 0.39, 0.8))
    bandax = plt.axes((0.56, 0.1, 0.39, 0.8), sharey=telax)
    for ii, (psrid, info) in enumerate(pulsars.iteritems()):
        indices.append(ii)
        labels.append(utils.get_pulsarname(psrid))
        total = 0
        for telname in sorted(telescopes):
            if telname in info:
                count = info[telname]
                bb = telax.barh(ii, count, height=1, left=total, \
                            color=telescope_colours[telname])
                telleg[telname] = bb
                total += count
        total = 0
        for band in ['P-band', 'L-band', 'S-band']:
            if band in info:
                count = info[band]
                bb = bandax.barh(ii, count, height=1, left=total, \
                            color=band_colours[band])
                bandleg[band] = bb
                total += count
    telax.yaxis.set_ticks(np.array(indices)+0.5)
    telax.yaxis.set_ticklabels(labels)
    telax.set_ylim(-0.5, len(pulsars)+2.5)
    plt.setp((telax.get_xticklabels(), \
              bandax.get_xticklabels()), \
             rotation=30, ha='right')
    telax.set_xlabel("Number of TOAs")
    bandax.set_xlabel("Number of TOAs")
    telax.set_title("By telescope")
    bandax.set_title("By observing band")
    labels, handles = zip(*sorted(telleg.items()))
    telax.legend(handles, labels, prop=dict(size='small'))
    handles, labels = zip(*bandleg.items())
    bandax.legend(labels, handles, prop=dict(size='small'))
    plt.setp(bandax.yaxis.get_ticklabels(), visible=False)
    return fig


def main(args):
    toas = create_timfile.get_toas(args)
    if not len(toas):
        raise errors.ToasterError("No TOAs to give an overview of.")
    if args.output_style=='histogram':
        import matplotlib.pyplot as plt
        fig = plot_toa_histogram(toas)
        plt.show()
    elif args.output_style=='cadence':
        import matplotlib.pyplot as plt
        fig = plot_cadence(toas)
        plt.show()
    #elif args.output_style=='summary':
    #    summarize_toas(toas)
    else:
        raise errors.UnrecognizedValueError("The output style '%s' " \
                        "is not recognized." % args.output_style)


if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
