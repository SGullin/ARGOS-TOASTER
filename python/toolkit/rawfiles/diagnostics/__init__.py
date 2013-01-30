import database
import utils
import errors

registered_diagnostics = ['composite']


def insert_diagnostics(diags, rawfile_id, existdb=None):
    """Given a list of diagnostic names compute and insert
        diagnostics to the database.

        Inputs:
            diags: A list of diagnostic names.
            rawfile_id: The ID of the rawfile to compute diagnostics for.
            existdb: An (optional) existing database connection object.
                (Default: Establish a db connection)

        Outputs:
            None
    """
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    try:
        for diag_name in diags:
            if diag_name not in registered_diagnostics:
                raise errors.UnrecognizedValueError("The diagnostic, '%s', " \
                            "is not a registered. The following " \
                            "are registered: '%s'" % \
                            (diag_name, "', '".join(registered_diagnostics)))
            mod = __import__(diag_name, globals())
            diag = mod.Diagnostic()
            diag.insert(rawfile_id)
    finally:
        if not existdb:
            # Close DB connection
            db.close()
