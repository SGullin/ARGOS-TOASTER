#!/usr/bin/env python

"""
timfile.py

A multi-purpose program to interact with timfiles.

Patrick Lazarus, Dec 9, 2012
"""

import utils
import database
import errors

toolkit = ['create_timfile', \
           'describe_timfiles', \
           'write_timfile', \
           'edit_timfile', \
           'set_master_timfile', \
          ]


def main():
    args.func(args)


if __name__ == '__main__':
    parser = utils.DefaultArguments(prog='timfile.py', \
                            description='A multi-purpose program to interact ' \
                                'with timfiles.')
    subparsers = parser.add_subparsers(help='Available functionality. ' \
                            'To get more detailed help for each function ' \
                            'provide the "-h/--help" argument following the ' \
                            'function.')
    mod = __import__('toolkit.timfiles', globals(), locals(), toolkit)
    for tool_name in toolkit:
        tool = getattr(mod, tool_name)
        toolparser = subparsers.add_parser(tool.SHORTNAME, 
                        help=tool.DESCRIPTION)
        toolparser.set_defaults(func=tool.main)
        tool.add_arguments(toolparser)
    args = parser.parse_args()
    main()
