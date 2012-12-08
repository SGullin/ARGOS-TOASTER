#!/usr/bin/env python

"""
manage_templates.py

A multi-purpose program to manage templates.

Patrick Lazarus, Dec 5, 2012
"""

import utils
import database
import errors

toolkit = ['set_master_template', 'get_template_id', 'remove_template']


def main():
    args.func(args)


if __name__ == '__main__':
    parser = utils.DefaultArguments(prog='manage_templates.py', \
                            description='A multi-purpose program for managing ' \
                                'templates.')
    subparsers = parser.add_subparsers(help='Available functionality. ' \
                            'To get more detailed help for each function ' \
                            'provide the "-h/--help" argument following the ' \
                            'function.')
    mod = __import__('toolkit.templates', globals(), locals(), toolkit)
    for tool_name in toolkit:
        tool = getattr(mod, tool_name)
        toolparser = subparsers.add_parser(tool.SHORTNAME, 
                        help=tool.DESCRIPTION)
        toolparser.set_defaults(func=tool.main)
        tool.add_arguments(toolparser)
    args = parser.parse_args()
    main()
