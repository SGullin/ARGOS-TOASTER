#!/usr/bin/env python
"""
A script to add a rawfile diagnostic to the database/

Patrick Lazarus, Jan. 28, 2013
"""
import os.path
import argparse
import textwrap
import sys
import shlex
import warnings
import copy

import utils
import errors
import colour
import database
import diagnostics

SHORTNAME = 'diagnose'
DESCRIPTION = "Add a diagnostic value, or plot, for a rawfile."


def add_arguments(parser):
    parser.add_argument('-r', "--rawfile-id", dest='rawfile_id', type=int, \
                        default=None, \
                        help="Rawfile ID number of the data file the " \
                            "diagnostic describes.")
    parser.add_argument('--from-file', dest='from_file', \
                        type=str, default=None, \
                        help="A list of rawfiles (one per line) to " \
                            "diagnose. (Default: diagnose a raw file provided " \
                            "on the cmd line.)")
    parser.add_argument('-D', '--diagnostic', dest='diagnostic', type=str, \
                        help="Name of a diagnostic to add.")
    parser.add_argument('--list-diagnostics', nargs=0, \
                        action=ListDiagnosticsAction, \
                        help="List available diagnostics and exit.")
    parser.add_argument('-n', '--no-insert', dest='insert', \
                        action='store_false', default=True, \
                        help="Print diagnostic information to screen instead " \
                            "of inserting into the DB. (Default: Insert into " \
                            "DB.)")
    diaggroup = parser.add_mutually_exclusive_group(required=False)
    diaggroup.add_argument('--plot', metavar="PLOT", dest='value', \
                        type=str, default=None, \
                        help="Diagnostic plot to upload.")
    diaggroup.add_argument('--value', dest='value', type=float, \
                        default=None, \
                        help="Diagnostic (floating-point) value to upload.")


class ListDiagnosticsAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        colour.cprint("Available Diagnostics:", \
                        bold=True, underline=True) 
        descwrapper = textwrap.TextWrapper(initial_indent="    ", 
                                subsequent_indent="    ")
        for key in sorted(diagnostics.registered_diagnostics):
            diagcls = diagnostics.get_diagnostic_class(key)
            wrapper = textwrap.TextWrapper(subsequent_indent=" "*(len(key)+4))
            print "%s -- %s" % (colour.cstring(key, bold=True), 
                                    wrapper.fill(diagcls.name))
            if diagcls.description is not None:
                print descwrapper.fill(diagcls.description)
        sys.exit(1)


def diagnose_rawfile(rawfile_id, diagnostic, value=None, existdb=None):
    """Diagnose a rawfile (specified by its ID number).

        Inputs:
            rawfile_id: The ID number of the rawfile to diagnose.
            diagnostic: The name of the diagnostic.
            value: The value of the diagnostic.
                    - If the value provided is None, try to compute 
                        a pre-defined diagnostic. 
                    - If the value provided is a string, assume it 
                        is a plot-diagnostic. 
                    - If the value provided is a floating-point 
                        number, assume it is a numeric-diagnostic.
            existdb: An (optional) existing database connection object.
                (Default: Establish a db connection)
        
        Outputs:
            diag: The Diagnostic object.
    """
    db = existdb or database.Database()
    db.connect()
    
    fn = utils.get_rawfile_from_id(rawfile_id, existdb=db)
    if value is None:
        # Pre-defined diagnostic
        diagcls = diagnostics.get_diagnostic_class(diagnostic)
        # Pre-check if a diagnostic with this name already exists for
        # the rawfile provided
        check_rawfile_diagnostic_existence(rawfile_id, diagcls.name, \
                                            existdb=db)
        diag = diagcls(fn)
    elif type(value) == types.FloatType:
        # Numeric diagnostic
        utils.print_info("Custom floating-point rawfile diagnostic provided", 2)
        diag = diagnostics.get_custom_float_diagnostic(fn, diagnostic, \
                                        value)
    else:
        # Plot diagnostic
        utils.print_info("Custom rawfile diagnostic plot provided", 2)
        diag = diagnostics.get_custom_diagnostic_plot(fn, diagnostic, \
                                        value)
    if existdb is None:
        db.close()
    return diag


def check_rawfile_diagnostic_existence(rawfile_id, diagname, existdb=None):
    """Check a rawfile has a diagnostic (float-valued, or a plot) with
        the name provided. If a diagnostic exists raise a
        DiagnosticAlreadyExists error.
 
        Inputs:
            rawfile_id: The ID number of the raw file for which to 
                check diagnostics.
            diagname: The name of a registered diagnostic.
            existdb: An (optional) existing database connection object.
                (Default: Establish a db connection)
        
        Outputs:
            None
    """
    db = existdb or database.Database()
    db.connect()

    # Get float-valued diagnostics
    select = db.select([db.raw_diagnostics.c.raw_diagnostic_id, \
                        db.raw_diagnostics.c.value]).\
                where((db.raw_diagnostics.c.type==diagname) & \
                        (db.raw_diagnostics.c.rawfile_id==rawfile_id))
    result = db.execute(select)
    floatrows = result.fetchall()
    result.close()

    # Get plot-valued diagnostics
    select = db.select([db.raw_diagnostic_plots.c.raw_diagnostic_plot_id, \
                        db.raw_diagnostic_plots.c.filename, \
                        db.raw_diagnostic_plots.c.filepath]).\
                where((db.raw_diagnostic_plots.c.plot_type==diagname) & \
                        (db.raw_diagnostic_plots.c.rawfile_id==rawfile_id))
    result = db.execute(select)
    plotrows = result.fetchall()
    result.close()
    
    if existdb is None:
        db.close()
    if len(floatrows) > 1:
        raise errors.InconsistentDatabaseError("There should be no " \
                        "more than one float diagnostic of each type " \
                        "per candidate. There are %d '%s' diagnostic " \
                        "values for this rawfile (ID: %d)" % \
                        (len(floatrows), diagname, rawfile_id))
    elif len(floatrows) == 1:
        val = floatrows[0]['value']
        raise errors.DiagnosticAlreadyExists("There is already a '%s' float " \
                        "diagnostic value in the DB for this rawfile " \
                        "(ID: %d; value: %g)" % \
                        (diagname, rawfile_id, val))

    if len(plotrows) > 1:
        raise errors.InconsistentDatabaseError("There should be no " \
                        "more than one diagnostic plot of each type " \
                        "per candidate. There are %d '%s' diagnostic " \
                        "plots for this rawfile (ID: %d)" % \
                        (len(plotrows), diagname, rawfile_id))
    elif len(plotrows) == 1:
        fn = os.path.join(plotrows[0]['filepath'], plotrows[0]['filename'])
        raise errors.DiagnosticAlreadyExists("There is already a '%s' " \
                        "diagnostic plot in the DB for this rawfile " \
                        "(ID: %d; file: %s)" % \
                        (diagname, rawfile_id, fn))


def insert_rawfile_diagnostics(rawfile_id, diags, existdb=None):
    """Insert rawfile diagnostics, carefully checking if each
        diagnostic is float-valued, or plot-valued.
 
        Inputs:
            rawfile_id: The ID number of the raw file for which to 
                insert the diagnostic.
            diags: A list of computed diagnostics.
            existdb: An (optional) existing database connection object.
                (Default: Establish a db connection)
        
        Outputs:
            None
    """
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    utils.print_info("Computing raw file diagnostics for " \
                        "rawfile (ID: %d)" % rawfile_id, 2)

    try:
        for diag in diags:
            utils.print_info("Computing %s diagnostic" % diag.name, 3)
            
            trans = db.begin()
            try:
                check_rawfile_diagnostic_existence(rawfile_id, diag.name, \
                                                        existdb=db)
                if isinstance(diag, diagnostics.base.FloatDiagnostic):
                    __insert_rawfile_float_diagnostic(rawfile_id, diag, \
                                                        existdb=db)
                elif isinstance(diag, diagnostics.base.PlotDiagnostic):
                    __insert_rawfile_diagnostic_plot(rawfile_id, diag, \
                                                        existdb=db)
                else:
                    trans.rollback()
                    raise ValueError("Diagnostic is not a valid type (%s)!" % \
                                        type(diag))
            except errors.DiagnosticAlreadyExists, e:
                print_info("Diagnostic already exists: %s. Skipping..." % \
                                str(e), 2)
                trans.rollback()
            else:
                trans.commit()
    finally:
        if not existdb:
            # Close DB connection
            db.close()
        

def __insert_rawfile_float_diagnostic(rawfile_id, diag, existdb=None):
    """Insert rawfile float diagnostic.

        Inputs:
            rawfile_id: The ID number of the raw file for which to 
                insert the diagnostic.
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
        values = {'rawfile_id':rawfile_id, \
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

    
def __insert_rawfile_diagnostic_plot(rawfile_id, diag, existdb=None):
    """Insert rawfile plot diagnostic.

        Inputs:
            rawfile_id: The ID number of the raw file for which to 
                insert the diagnostic.
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
        archivedir = os.path.split(os.path.abspath(diag.fn))[0]
        diagpath = utils.archive_file(diagplot, archivedir)
        diagdir, diagfn = os.path.split(os.path.abspath(diagpath))
    
        ins = db.raw_diagnostic_plots.insert()
        values = {'rawfile_id':rawfile_id, \
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
    if (args.rawfile_id is None) and (args.from_file is None):
        warnings.warn("No input file or --from-file argument given " \
                        "will read from stdin.", \
                        errors.ToasterWarning)
        args.rawfile = None # In case it was set to '-'
        args.from_file = '-'
    # Connect to the database
    db = database.Database()
    db.connect()

    print args.from_file

    try:
        if args.from_file is not None:
            # Re-create parser, so we can read arguments from file
            parser = utils.DefaultArguments()
            add_arguments(parser)
            if args.rawfile_id is not None:
                raise errors.BadInputError("When diagnosing rawfiles " \
                                "using a file, a rawfile should _not_ " \
                                "be provided on the command line. (The " \
                                "value %s was given on the command line)." % \
                                args.rawfile_id)
            if args.from_file == '-':
                rawlist = sys.stdin
            else:
                if not os.path.exists(args.from_file):
                    raise errors.FileError("The rawfile list (%s) does " \
                                "not appear to exist." % args.from_file)
                rawlist = open(args.from_file, 'r')
            numfails = 0
            numdiagnosed = 0
            for line in rawlist:
                # Strip comments
                line = line.partition('#')[0].strip()
                if not line:
                    # Skip empty line
                    continue
                try:
                    customargs = copy.deepcopy(args)
                    arglist = shlex.split(line.strip())
                    parser.parse_args(arglist, namespace=customargs)
                    diag = diagnose_rawfile(customargs.rawfile_id, \
                                customargs.diagnostic, customargs.value, \
                                existdb=db)
                        
                    if customargs.insert:
                        insert_rawfile_diagnostics(customargs.rawfile_id, \
                                                [diag], existdb=db)
                    else:
                        print str(diag)
                except errors.ToasterError:
                    numfails += 1
                    traceback.print_exc()
            if args.from_file != '-':
                rawlist.close()
            if numdiagnosed:
                utils.print_success("\n\n===================================\n" \
                                    "%d rawfiles successfully diagnosed\n" \
                                    "===================================\n" % numadded)
            if numfails:
                raise errors.ToasterError(\
                    "\n\n===================================\n" \
                        "The diagnosis of %d rawfiles failed!\n" \
                        "Please review error output.\n" \
                        "===================================\n" % numfails)
        else:
            if not args.rawfile_id:
                raise ValueError("A rawfile ID number must be provided!")
            diag = diagnose_rawfile(args.rawfile_id, \
                        args.diagnostic, args.value, \
                        existdb=db)
                
            if args.insert:
                insert_rawfile_diagnostics(args.rawfile_id, \
                                        [diag], existdb=db)
            else:
                print str(diag)
    finally:
        db.close()


if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
