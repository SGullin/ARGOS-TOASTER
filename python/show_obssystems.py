#!/usr/bin/env python

"""
This TOASTER utility script provides the user with a listing
of observing systems in the database.

Patrick Lazarus, Nov 6, 2012.
"""
import errors
import colour
import utils


def main():
    # Build cache
    obssysinfo_cache = utils.get_obssysinfo_cache()

    obssys_ids = obssysinfo_cache.keys()

    print "--"*25
    for id in sorted(obssys_ids):
        obssysinfo = utils.get_obssysinfo(id)
        print colour.cstring("Observing System ID:", underline=True, bold=True) + \
                colour.cstring(" %d" % id, bold=True)
        print "Observing System Name: %s" % obssysinfo['name']
        print "Telescope: %d" % obssysinfo['telescope_id']
        print "Receiver: %s" % obssysinfo['frontend']
        print "Backend: %s" % obssysinfo['backend']
        print "Observing Band: %s" % obssysinfo['band_descriptor']
        print "--"*25


if __name__=='__main__':
    parser = utils.DefaultArguments(description="Get a listing of observing " \
                                    "systems from the DB to help the user.")
    args = parser.parse_args()
    main()
