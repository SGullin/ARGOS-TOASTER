#!/usr/bin/env python

"""
parfiles.py

A multi-purpose program to interact with parfiles.

Patrick Lazarus, Dec 8, 2012
"""

import utils
import database
import errors

toolkit = ['set_master_parfile', \
           'get_parfile_id', \
           'remove_parfile', \
           'load_parfile', \
          ]


def main():
    args.func(args)


if __name__ == '__main__':
    parser = utils.DefaultArguments(prog='parfiles.py', \
                            description='A multi-purpose program to interact ' \
                                'with parfiles.')
    subparsers = parser.add_subparsers(help='Available functionality. ' \
                            'To get more detailed help for each function ' \
                            'provide the "-h/--help" argument following the ' \
                            'function.')
    mod = __import__('toolkit.parfiles', globals(), locals(), toolkit)
    for tool_name in toolkit:
        tool = getattr(mod, tool_name)
        toolparser = subparsers.add_parser(tool.SHORTNAME, 
                        help=tool.DESCRIPTION)
        toolparser.set_defaults(func=tool.main)
        tool.add_arguments(toolparser)
    args = parser.parse_args()
    main()
