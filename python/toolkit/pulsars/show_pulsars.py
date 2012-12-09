#!/usr/bin/env python

"""
This TOASTER utility script provides the user with a listing
of pulsars in the database.

Patrick Lazarus, Oct 28, 2012.
"""
import errors
import colour
import utils


SHORTNAME = 'show'
DESCRIPTION = "Get a listing of pulsars " \
              "from the DB to help the user."


def add_arguments(parser):
    parser.add_argument('-p', '--psr', dest='psrnames', \
                        type=str, default=[], action='append', \
                        help="The pulsar to grab info for.")
    parser.add_argument('--pulsar-id', dest='pulsar_ids', \
                        type=int, default=[], action='append', \
                        help="IDs of pulsars to grab info for.")
    parser.add_argument("--output-style", default='text', \
                        dest='output_style', type=str, \
                        help="The following options control how " \
                        "pulsars are displayed. Recognized " \
                        "modes: 'text' - List pulsars and aliases in a " \
                        "human-readable format; 'dump' - Dump all " \
                        "pulsar names and aliases to screen. " \
                        "(Default: text).")


def show_pulsars(pulsar_ids):
    """Print pulsars and aliases to screen in a human-readable
        format.

        Input:
            pulsar_ids: list of pulsar IDs to display.

        Outputs:
            None
    """
    # Grab the pulsar alias cache once rather than accessing it multiple times
    pulsaralias_cache = utils.get_pulsaralias_cache()
    print "--"*25
    for id in sorted(pulsar_ids):
        psrname = utils.get_pulsarname(id)
        print colour.cstring("Pulsar ID:", underline=True, bold=True) + \
                colour.cstring(" %d" % id, bold=True)
        print "Pulsar Name: %s" % psrname 
        print "Aliases:"
        for alias in pulsaralias_cache[id]:
            if alias == psrname:
                continue
            print "    %s" % alias
        print "--"*25


def dump_pulsars(pulsar_ids):
    """Dump pulsar names and aliases to screen.

        Input:
            pulsar_ids: list of pulsar IDs to display.

        Outputs:
            None
    """
    # Grab the pulsar alias cache once rather than accessing it multiple times
    pulsaralias_cache = utils.get_pulsaralias_cache()
    for id in sorted(pulsar_ids):
        psrname = utils.get_pulsarname(id)
        print psrname
        for alias in pulsaralias_cache[id]:
            if alias == psrname:
                continue
            print alias



def main(args):
    # Build caches
    utils.get_pulsarname_cache()
    pulsar_ids = args.pulsar_ids + \
                    [utils.get_pulsarid(psr) for psr in args.psrnames]
    if not pulsar_ids:
        pulsaralias_cache = utils.get_pulsaralias_cache()
        pulsar_ids = pulsaralias_cache.keys()

    if args.output_style=='text':
        show_pulsars(pulsar_ids)
    elif args.output_style=='dump':
        dump_pulsars(pulsar_ids)
    else:
        raise errors.UnrecognizedValueError("The output-style '%s' is " \
                    "not recognized!" % args.output_style)


if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
