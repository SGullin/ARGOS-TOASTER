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
    print "--"*25
    for id in sorted(pulsaralias_cache.keys()):
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
    args = parser.parse_args()
    main()
