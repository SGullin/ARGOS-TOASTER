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
    parser.add_argument('-n', '--no-insert', dest='insert', \
                        action='store_false', default=True, \
                        help="Print diagnostic information to screen instead " \
                            "of inserting into the DB. (Default: Insert into " \
                            "DB.)")
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

def insert_rawfile_diagnostics(diags, existdb=None):
    """Insert rawfile diagnostics, carefully checking if each
        diagnostic is float-valued, or plot-valued.
 
        Inputs:
            diags: A list of computed diagnostics.
            existdb: An (optional) existing database connection object.
                (Default: Establish a db connection)
        
        Outputs:
            None
    """
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    try:
        for diag in diags:
            if isinstance(diag, diagnostics.base.FloatDiagnostic):
                __insert_rawfile_float_diagnostic(diag, existdb=db)
            elif isinstance(diag, diagnostics.base.PlotDiagnostic):
                __insert_rawfile_diagnostic_plot(diag, existdb=db)
            else:
                raise ValueError("Diagnostic is not a valid type (%s)!" % \
                                    type(diag))
    finally:
        if not existdb:
            # Close DB connection
            db.close()
        

def __insert_rawfile_float_diagnostic(diag, existdb=None):
    """Insert rawfile float diagnostic.

        Inputs:
            diag: A FloatDiagnostic object.
            existdb: An (optional) existing database connection object.
                (Default: Establish a db connection)
        
        Outputs:
            None
    """
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    try:
        ins = db.raw_diagnostics.insert()
        values = {'rawfile_id':diag.rawfile_id, \
                  'value':diag.diagnostic, \
                  'type':diag.name}
        result = db.execute(ins, values)
        diag_id = result.inserted_primary_key[0]
        result.close()
        utils.print_info("Inserted rawfile diagnostic (type: %s)." % \
                    diag.name, 2)
    finally:
        if not existdb:
            # Close DB connection
            db.close()
    return diag_id

    
def __insert_rawfile_diagnostic_plot(diag, existdb=None):
    """Insert rawfile plot diagnostic.

        Inputs:
            diag: A FloatDiagnostic object.
            existdb: An (optional) existing database connection object.
                (Default: Establish a db connection)
        
        Outputs:
            None
    """
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    diagpath = None # Initialise diagplot in case an exception is raised
    try:
        # Put diagnostic plot next to the data file
        diagplot = os.path.abspath(diag.diagnostic)
        archivedir = os.path.split(diagplot)[0]
        diagpath = utils.archive_file(diagplot, archivedir)
        diagdir, diagfn = os.path.split(os.path.abspath(diagpath))
    
        ins = db.raw_diagnostic_plots.insert()
        values = {'rawfile_id':diag.rawfile_id, \
                  'filename':diagfn, \
                  'filepath':diagdir, \
                  'plot_type':diag.name}
        result = db.execute(ins, values)
        diag_id = result.inserted_primary_key[0]
        result.close()
        utils.print_info("Inserted rawfile diagnostic plot (type: %s)." % \
                    diag.name, 2)
    except:
        # Move the diagnostic plot back if it has already been archived.
        if diagpath and os.path.isfile(diagpath):
            shutil.move(diagpath, diagplot)
        raise
    finally:
        if not existdb:
            # Close DB connection
            db.close()
    return diag_id


def main(args):
    if not args.rawfile_id:
        raise ValueError("A rawfile ID number must be provided!")
    if args.plot is not None:
        utils.print_info("Custom rawfile diagnostic plot provided", 2)
        diag = diagnostics.get_custom_diagnostic_plot(args.diagnostic, \
                                        args.plot, args.rawfile_id)
    elif args.value is not None:
        utils.print_info("Custom floating-point rawfile diagnostic provided", 2)
        diag = diagnostics.get_custom_float_diagnostic(args.diagnostic, \
                                        args.value, args.rawfile_id)
    else:
        diag = diagnostics.get_diagnostic(args.diagnostic, args.rawfile_id)
    if args.insert:
        insert_rawfile_diagnostics([diag])
    else:
        print str(diag)


if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
