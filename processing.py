#!/usr/bin/env python

"""
processing.py

A multi-purpose program to interact with processing results.

Patrick Lazarus, Feb. 7, 2012
"""

import utils
import database
import errors

toolkit = ['describe_processing', \
          ]


def main():
    args.func(args)


if __name__ == '__main__':
    parser = utils.DefaultArguments(prog='process.py', \
                            description='A multi-purpose program to interact ' \
                                'with processing results.')
    subparsers = parser.add_subparsers(help='Available functionality. ' \
                            'To get more detailed help for each function ' \
                            'provide the "-h/--help" argument following the ' \
                            'function.')
    mod = __import__('toolkit.processing', globals(), locals(), toolkit)
    for tool_name in toolkit:
        tool = getattr(mod, tool_name)
        toolparser = subparsers.add_parser(tool.SHORTNAME, 
                        help=tool.DESCRIPTION)
        toolparser.set_defaults(func=tool.main)
        tool.add_arguments(toolparser)
    args = parser.parse_args()
    main()

