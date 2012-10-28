#!/usr/bin/env python

"""
This TOASTER utility script provides the user with a listing
of pulsars in the database.

Patrick Lazarus, Oct 28, 2012.
"""
import errors
import colour
import utils


def main():
    # Build caches
    utils.get_pulsarname_cache()
    pulsaralias_cache = utils.get_pulsaralias_cache()
    pulsar_ids = args.pulsar_ids + \
                    [utils.get_pulsarid(psr) for psr in args.psrnames]
    if not pulsar_ids:
        pulsar_ids = pulsaralias_cache.keys()
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
    


if __name__=='__main__':
    parser = utils.DefaultArguments(description="Get a listing of pulsars " \
                                        "from the DB to help the user.")
    parser.add_argument('-p', '--psr', dest='psrnames', \
                        type=str, default=[], action='append', \
                        help="The pulsar to grab info for.")
    parser.add_argument('--pulsar-id', dest='pulsar_ids', \
                        type=int, default=[], action='append', \
                        help="IDs of pulsars to grab info for.")
    args = parser.parse_args()
    main()
