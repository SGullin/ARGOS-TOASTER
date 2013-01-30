import os.path

import utils
import database

class FloatDiagnostic(object):
    """The base class for floating-point valued diagnostics.
    """
    name = NotImplemented
    def _compute(self, fn):
        raise NotImplementedError("The compute method must be defined by " \
                                    "subclasses of FloatDiagnostic.")
    
    def insert(self, rawfile_id, existdb=None):
        """Insert rawfile plot diagnostics.
 
            Inputs:
                rawfile_id: The ID number of the rawfile that is being diagnosed
                existdb: An (optional) existing database connection object.
                    (Default: Establish a db connection)
            
            Outputs:
                None
        """
        # Connect to the database
        db = existdb or database.Database()
        db.connect()
 
        fn = utils.get_rawfile_from_id(rawfile_id, db)
        diagval = self._compute(fn) 
        
        try:
            ins = db.raw_diagnostics.insert()
            values = {'rawfile_id':rawfile_id, \
                      'value':diagval, \
                      'type':self.name}
            result = db.execute(ins, values)
            result.close()
            utils.print_info("Inserted rawfile diagnostic (type: %s)." % \
                        self.name, 2)
        finally:
            if not existdb:
                # Close DB connection
                db.close()


class PlotDiagnostic(object):
    """The base class for plot diagnostics.
    """
    name = NotImplemented
    def _compute(self, fn):
        raise NotImplementedError("The compute method must be defined by " \
                                    "subclasses of PlotDiagnostic.")

    def insert(self, rawfile_id, existdb=None):
        """Insert rawfile plot diagnostics.
 
            Inputs:
                rawfile_id: The ID number of the rawfile that is being diagnosed
                existdb: An (optional) existing database connection object.
                    (Default: Establish a db connection)
            
            Outputs:
                None
        """
        # Connect to the database
        db = existdb or database.Database()
        db.connect()
 
        fn = utils.get_rawfile_from_id(rawfile_id, db)
        diagfn = self._compute(fn)
        # Put diagnostic plot next to the data file
        archivedir = os.path.split(os.path.abspath(fn))[0]
        diagpath = utils.archive_file(diagfn, archivedir)
        diagdir, diagfn = os.path.split(os.path.abspath(diagpath))
        
        try:
            ins = db.raw_diagnostic_plots.insert()
            values = {'rawfile_id':rawfile_id, \
                      'filename':diagfn, \
                      'filepath':diagdir, \
                      'plot_type':self.name}
            result = db.execute(ins, values)
            result.close()
            utils.print_info("Inserted rawfile diagnostic plot (type: %s)." % \
                        self.name, 2)
        finally:
            if not existdb:
                # Close DB connection
                db.close()

