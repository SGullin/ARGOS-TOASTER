#!/usr/bin/env python

"""
pulsar.py

A multi-purpose program to interact with pulsar.

Patrick Lazarus, Dec 9, 2012
"""

import utils
import database
import errors

toolkit = ['add_pulsar', \
           'show_pulsars', \
           'rename_pulsar', \
           'add_pulsar_alias', \
           'merge_pulsar', \
          ]


def main():
    args.func(args)


if __name__ == '__main__':
    parser = utils.DefaultArguments(prog='pulsar.py', \
                            description='A multi-purpose program to interact ' \
                                'with pulsar.')
    subparsers = parser.add_subparsers(help='Available functionality. ' \
                            'To get more detailed help for each function ' \
                            'provide the "-h/--help" argument following the ' \
                            'function.')
    mod = __import__('toolkit.pulsars', globals(), locals(), toolkit)
    for tool_name in toolkit:
        tool = getattr(mod, tool_name)
        toolparser = subparsers.add_parser(tool.SHORTNAME, 
                        help=tool.DESCRIPTION)
        toolparser.set_defaults(func=tool.main)
        tool.add_arguments(toolparser)
    args = parser.parse_args()
    main()
