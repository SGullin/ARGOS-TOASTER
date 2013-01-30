#!/usr/bin/env python
"""
A script to add a rawfile diagnostic to the database/

Patrick Lazarus, Jan. 28, 2013
"""

import utils
import errors
import database
import diagnostics

SHORTNAME = 'diagnose'
DESCRIPTION = "Add a diagnostic value, or plot, for a rawfile."


def add_arguments(parser):
    parser.add_argument('-r', "--rawfile-id", dest='rawfile_id', type=int, \
                        help="Rawfile ID number of the data file the " \
                            "diagnostic describes.")
    parser.add_argument('-D', '--diagnostic', dest='diagnostic', type=str, \
                        help="Name of a diagnostic to add.")
#    diaggroup = parser.add_mutually_exclusive_group(required=False)
#    diaggroup.add_argument('--plot', dest='plot', type=str, \
#                        default=None, \
#                        help="Diagnostic plot to upload.")
#    diaggroup.add_argument('--value', dest='value', type=float, \
#                        default=None, \
#                        help="Diagnostic (floating-point) value to upload.")
    parser.add_argument('--from-file', dest='from_file', \
                        type=str, default=None, \
                        help="A list of diagnostics (one per line) to " \
                            "load. (Default: load a diagnostic provided " \
                            "on the cmd line.)")


def main(args):
    if not args.rawfile_id:
        raise ValueError("A rawfile ID number must be provided!")
    diagnostics.insert_diagnostics([args.diagnostic], args.rawfile_id)


if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
