"""
A script to add processing diagnostics to the database

Patrick Lazarus, Feb 8, 2013
"""
import os.path

import utils
import errors
import database
import diagnostics


def check_processing_diagnostic_existence(proc_id, diagname, existdb=None):
    """Check if processing run has a diagnostic (float-valued, or a plot) 
        with the name provided. If a diagnostic exists raise a
        DiagnosticAlreadyExists error.
 
        Inputs:
            proc_id: The ID number of the processing for which to 
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
    select = db.select([db.proc_diagnostics.c.proc_diagnostic_id, \
                        db.proc_diagnostics.c.value]).\
                where((db.proc_diagnostics.c.type==diagname) & \
                        (db.proc_diagnostics.c.process_id==proc_id))
    result = db.execute(select)
    floatrows = result.fetchall()
    result.close()

    # Get plot-valued diagnostics
    select = db.select([db.proc_diagnostic_plots.c.proc_diagnostic_plot_id, \
                        db.proc_diagnostic_plots.c.filename, \
                        db.proc_diagnostic_plots.c.filepath]).\
                where((db.proc_diagnostic_plots.c.plot_type==diagname) & \
                        (db.proc_diagnostic_plots.c.process_id==proc_id))
    result = db.execute(select)
    plotrows = result.fetchall()
    result.close()
    
    if existdb is None:
        db.close()
    if len(floatrows) > 1:
        raise errors.InconsistentDatabaseError("There should be no " \
                        "more than one float diagnostic of each type " \
                        "per processing job. There are %d '%s' diagnostic " \
                        "values for this processing job (ID: %d)" % \
                        (len(floatrows), diagname, proc_id))
    elif len(floatrows) == 1:
        val = floatrows[0]['value']
        raise errors.DiagnosticAlreadyExists("There is already a '%s' float " \
                        "diagnostic value in the DB for this processing job " \
                        "(ID: %d; value: %g)" % \
                        (diagname, rawfile_id, val))

    if len(plotrows) > 1:
        raise errors.InconsistentDatabaseError("There should be no " \
                        "more than one diagnostic plot of each type " \
                        "per processing job. There are %d '%s' diagnostic " \
                        "plots for this processing job (ID: %d)" % \
                        (len(plotrows), diagname, proc_id))
    elif len(plotrows) == 1:
        fn = os.path.join(plotrows[0]['filepath'], plotrows[0]['filename'])
        raise errors.DiagnosticAlreadyExists("There is already a '%s' " \
                        "diagnostic plot in the DB for this processing job " \
                        "(ID: %d; file: %s)" % \
                        (diagname, proc_id, fn))


def insert_processing_diagnostics(proc_id, diags, archivedir=None, \
                                    suffix="", existdb=None):
    """Insert processing diagnostics, carefully checking if each
        diagnostic is float-valued, or plot-valued.
 
        Inputs:
            proc_id: The ID number of the processing job for which
                the diagnostic describes.
            diags: A list of computed diagnostics.
            archivedir: The location where diagnostic plots should be
                archived. (Default: put diagnostic plots in same directory
                as the input file.)
            suffix: Add a suffix just before the extension of diagnostic
                plots' filenames. (Default: Do not insert a suffix)
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
            trans = db.begin()
            try:
                if isinstance(diag, diagnostics.base.FloatDiagnostic):
                    __insert_processing_float_diagnostic(proc_id, diag, existdb=db)
                elif isinstance(diag, diagnostics.base.PlotDiagnostic):
                    __insert_processing_diagnostic_plot(proc_id, diag, \
                                                    archivedir=archivedir, \
                                                    suffix=suffix, existdb=db)
                else:
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
        

def __insert_processing_float_diagnostic(proc_id, diag, existdb=None):
    """Insert processing float diagnostic.

        Inputs:
            proc_id: The ID number of the processing job for which
                the diagnostic describes.
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
        ins = db.proc_diagnostics.insert()
        values = {'process_id':proc_id, \
                  'value':diag.diagnostic, \
                  'type':diag.name}
        result = db.execute(ins, values)
        diag_id = result.inserted_primary_key[0]
        result.close()
        utils.print_info("Inserted process diagnostic (type: %s)." % \
                    diag.name, 2)
    finally:
        if not existdb:
            # Close DB connection
            db.close()
    return diag_id

    
def __insert_processing_diagnostic_plot(proc_id, diag, archivedir=None, \
                                    suffix="", existdb=None):
    """Insert processing plot diagnostic.

        Inputs:
            proc_id: The ID number of the processing job for which
                the diagnostic describes.
            diag: A PlotDiagnostic object.
            archivedir: The location where diagnostic plots should be
                archived. (Default: put diagnostic plots in same directory
                as the input file.)
            suffix: Add a suffix just before the extension of diagnostic
                plots' filenames. (Default: Do not insert a suffix)
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
        origfn = os.path.abspath(diag.diagnostic)
        if archivedir is None:
            archivedir = os.path.split(os.path.abspath(diag.fn))[0]
        if suffix:
            # Rename file
            stem, ext = os.path.splitext(diagplot)
            newfn = stem+suffix+ext
            if os.path.dirname(newfn) != os.path.dirname(diagplot):
                raise errors.FileError("Adding processing diagnostic " \
                            "plot suffix will cause plot to be moved " \
                            "to annother directory (new file name: %s)!" % \
                            newfn)
            shutil.move(diagplot, newfn)
            diagplot = newfn
        else:
            diagplot = origfn
        diagpath = utils.archive_file(diagplot, archivedir)
        diagdir, diagfn = os.path.split(os.path.abspath(diagpath))
    
        ins = db.proc_diagnostic_plots.insert()
        values = {'process_id':proc_id, \
                  'filename':diagfn, \
                  'filepath':diagdir, \
                  'plot_type':diag.name}
        result = db.execute(ins, values)
        diag_id = result.inserted_primary_key[0]
        result.close()
        utils.print_info("Inserted process diagnostic plot (type: %s)." % \
                    diag.name, 2)
    except:
        # Move the diagnostic plot back if it has already been archived.
        if diagpath and os.path.isfile(diagpath):
            shutil.move(diagpath, origfn)
        raise
    finally:
        if not existdb:
            # Close DB connection
            db.close()
    return diag_id
