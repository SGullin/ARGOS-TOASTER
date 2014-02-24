#!/usr/bin/env python

from toaster import utils
from toaster.utils import cache
from toaster.toolkit.pulsars import add_pulsar as ap

SHORTNAME = 'addalias'
DESCRIPTION = "Add a new alias for a pulsar entry."


def add_arguments(parser):
    parser.add_argument('-a', '--alias', dest='aliases',
                        type=str, action='append', default=[],
                        help="An alias for the pulsar. NOTE: multiple "
                             "aliases may be provided by including "
                             "multiple -a/--alias flags.")
    parser.add_argument('-p', '--psr', dest='psrname', type=str,
                        help="The pulsar to rename.")


def main(args):
    pulsar_id = cache.get_pulsarid(args.psrname)
    ap.add_pulsar_aliases(pulsar_id, args.aliases)


if __name__ == '__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)