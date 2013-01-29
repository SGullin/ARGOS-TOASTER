#!/usr/bin/env python
"""
A script to add a rawfile diagnostic to the database/

Patrick Lazarus, Jan. 28, 2013
"""

import utils
import errors
import database


SHORTNAME = 'diagnose'
DESCRIPTION = "Add a diagnostic value, or plot, for a rawfile."


def add_arguments(parser):
    parser.add_argument('-r', "--rawfile-id", dest='rawfile_id', type=int, \
                        help="Rawfile ID number of the data file the " \
                            "diagnostic describes.")
    parser.add_argument('-D', '--diagnostic', dest='diagnostic', type=str, \
                        help="Name of a diagnostic to add.")
    diaggroup = parser.add_mutually_exclusive_group(required=False)
    diaggroup.add_argument('--plot', dest='plot', type=str, \
                        default=None, \
                        help="Diagnostic plot to upload.")
    diaggroup.add_argument('--value', dest='value', type=float, \
                        default=None, \
                        help="Diagnostic (floating-point) value to upload.")
    parser.add_argument('--from-file', dest='from_file', \
                        type=str, default=None, \
                        help="A list of diagnostics (one per line) to " \
                            "load. (Default: load a diagnostic provided " \
                            "on the cmd line.)")


def insert_rawfile_diagnostics(rawfile_id, diags, existdb=None):
    """Insert rawfile plot diagnostics.

        Inputs:
            rawfile_id: The ID number of the rawfile that is being diagnosed
            diagfns: A dictionary of diagnostics. The keys
                are the corresponding diagnostic types.
            existdb: An (optional) existing database connection object.
                (Default: Establish a db connection)
        
        Outputs:
            None
    """
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    try:
        for diagtype, diagval in diags.iteritems():
            ins = db.raw_diagnostics.insert()
            values = {'rawfile_id':rawfile_id, \
                      'value':diagval, \
                      'type':diagtype}
            result = db.execute(ins, values)
            result.close()
            utils.print_info("Inserted rawfile diagnostic (type: %s)." % \
                        diagtype, 2)
    finally:
        if not existdb:
            # Close DB connection
            db.close()


def insert_rawfile_diagnostic_plots(rawfile_id, diagfns, existdb=None):
    """Insert rawfile plot diagnostics.

        Inputs:
            rawfile_id: The ID number of the rawfile that is being diagnosed
            diagfns: A dictionary of diagnostic filenames. The keys
                are the corresponding diagnostic types.
            existdb: An (optional) existing database connection object.
                (Default: Establish a db connection)
        
        Outputs:
            None
    """
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    try:
        for diagtype, diagpath in diagfns.iteritems():
            if diagpath is None:
                continue
            diagdir, diagfn = os.path.split(os.abspath(diagpath))
            ins = db.raw_diagnostic_plots.insert()
            values = {'rawfile_id':rawfile_id, \
                      'filename':diagfn, \
                      'filepath':diagdir, \
                      'plot_type':diagtype}
            result = db.execute(ins, values)
            result.close()
            utils.print_info("Inserted rawfile diagnostic plot (type: %s)." % \
                        diagtype, 2)
    finally:
        if not existdb:
            # Close DB connection
            db.close()


def main(args):
    print args


if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
