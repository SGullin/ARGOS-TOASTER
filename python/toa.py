#!/usr/bin/env python

"""
toa.py

A multi-purpose program to interact with TOAs.

Patrick Lazarus, Dec 26, 2012
"""

import utils
import database
import errors

toolkit = ['comment_toa', \
           'load_toa', \
          ]


def main():
    args.func(args)


if __name__ == '__main__':
    parser = utils.DefaultArguments(prog='toa.py', \
                            description='A multi-purpose program to interact ' \
                                'with TOAs.')
    subparsers = parser.add_subparsers(help='Available functionality. ' \
                            'To get more detailed help for each function ' \
                            'provide the "-h/--help" argument following the ' \
                            'function.')
    mod = __import__('toolkit.toas', globals(), locals(), toolkit)
    for tool_name in toolkit:
        tool = getattr(mod, tool_name)
        toolparser = subparsers.add_parser(tool.SHORTNAME, 
                        help=tool.DESCRIPTION)
        toolparser.set_defaults(func=tool.main)
        tool.add_arguments(toolparser)
    args = parser.parse_args()
    main()

