import warnings
import sqlalchemy as sa

import errors
import config
import schema
import epta_pipeline_utils as epu


# The following will execute the PRAGMA every time a connection
# is established. The PRAGMA is required to turn on foreign
# key support for sqlite databases.
@sa.event.listens_for(sa.engine.Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


class Database(object):
    def __init__(self, autocommit=True, url=config.dburl, *args, **kwargs):
        """Set up a Toaster Database object using SQLAlchemy.
        """
        # Create the database engine
        self.engine = sa.create_engine(url, *args, **kwargs)
        self.conn = None # No connection is established 
                         # until self.connect() is called
        sa.event.listen(self.engine, "before_cursor_execute", \
                            self.before_cursor_execute)
        self.autocommit = autocommit

        # The database description (metadata)
        self.metadata = schema.metadata
        self.tables = self.metadata.tables

    def __del__(self):
        self.close()

    def get_table(self, tablename):
        return self.tables[tablename]

    def __getitem__(self, key):
        return self.get_table(key)

    def __getattr__(self, key):
        return self.get_table(key)

    def is_connected(self):
        """Return True if an open connection is established.

            Inputs:
                None

            Output:
                isconn: True if an open connection is established.
        """
        return self.conn and not self.conn.closed

    def is_created(self):
        """Return True if the database appears to be setup
            (i.e. it has tables).

            Inputs:
                None

            Output:
                is_setup: True if the database is set up, False otherwise.
        """
        return bool(self.engine.table_names())

    def connect(self):
        """Connect to the database, setting self.conn.
            
            Inputs:
                None

            Output:
                conn: The established SQLAlchemy Connection object, 
                    which is also available as self.conn.
        """
        if not self.is_created():
            raise errors.DatabaseError("The database (%s) does not appear " \
                                        "to have any tables. Be sure to run " \
                                        "'create_tables.py' before attempting " \
                                        "to connect to the database." % \
                                                self.engine.url.database)
        # Only open a connection if not already connected
        if not self.is_connected():
            # Establish a connection
            self.conn = self.engine.connect()
            self.conn.execution_options(autocommit=self.autocommit)
            self.open_transactions = []
            self.result = None
        return self.conn

    def before_cursor_execute(self, conn, cursor, statement, parameters, \
                                context, executemany):
        """An event to be executed before execution of SQL queries.

            See SQLAlchemy for details about event triggers.
        """
        # Step back 7 levels through the call stack to find
        # the function that called 'execute'
        msg = str(statement)
        if executemany and len(parameters) > 1:
            msg += "\n    Executing %d statements" % len(parameters)
        elif parameters:
            msg += "\n    Params: %s" % str(parameters)
        epu.print_debug(msg, "queries", stepsback=7)

    def execute(self, *args, **kwargs):
        """Execute a query.

            Inputs:
                ** Allowable inputs are the same as defined
                    by SQLAlchemy's Connection object's 'execute'
                    method.
                ** Input arguments are passed directly to
                    self.conn.execute(...)

            Output:
                result: The SQLAlchemy ResultProxy object returned
                    by the call to self.conn.execute(...).
        """
        if not self.is_connected():
            raise errors.DatabaseError("Connection to database not " \
                    "established. Be sure self.connect(...) is called " \
                    "before attempting to execute queries.")
        if self.result is not None:
            self.result.close()
        self.result = self.conn.execute(*args, **kwargs)
        return self.result

    def begin(self):
        """Begin a transaction.

            Inputs:
                None

            Outputs:
                None
        """
        if self.open_transactions:
            warnings.warn("A transaction already appears to be in progress.", \
                           errors.EptaPipelineWarning) 
        trans = self.conn.begin()
        self.open_transactions.append(trans)

    def commit(self):
        """Commit the most recently opened transaction.
            
            Inputs:
                None

            Outputs:
                None
        """
        if self.open_transactions:
            trans = self.open_transactions.pop()
        else:
            raise errors.DatabaseError("Cannot commit. No open database transactions.")
        trans.commit()

    def rollback(self):
        """Roll back the most recently opened transaction.
            
            Inputs:
                None

            Outputs:
                None
        """
        trans = self.open_transactions.pop()
        trans.rollback()

    def close(self):
        """Close the established connection. 
            Also, close any result set that may be present.

            Inputs:
                None

            Outputs:
                None
        """
        if self.is_connected():
            self.conn.close()
            if self.result is not None:
                self.result.close()

    def fetchone(self):
        """Fetch and return a single row from the
            current result set.

            Inputs:
                None

            Output:
                row: The next row, or None if there are no
                    more rows in the result set.
        """
        return self.result.fetchone()

    def fetchall(self):
        """Fetch and return all rows from the
            current result set.

            Inputs:
                None

            Output:
                rows: The rows of the result set.
        """
        return self.result.fetchall()

    def execute_and_fetchone(self, *args, **kwargs):
        """A convenience method for executing a query and
            fetching a single row.
            
            See self.execute(...) and self.fetchone(...) for
            more documentation
        """
        self.execute(*args, **kwargs)
        return self.fetchone()

    def execute_and_fetchall(self):
        """A convenience method for executing a query and
            fetching all rows.
            
            See self.execute(...) and self.fetchall(...) for
            more documentation
        """
        self.execute(*args, **kwargs)
        return self.fetchall()
       
    @staticmethod
    def select(*args, **kwargs):
        """A staticmethod for returning a select object.

            Inputs:
                ** All arguments are directly passed to 
                    'sqlalchemy.sql.select'.

            Outputs:
                select: The select object returned by \
                    'sqlalchemy.sql.select'.
        """      
        return sa.sql.select(*args, **kwargs)
