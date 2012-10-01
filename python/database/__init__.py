import warnings
import sqlalchemy as sa

import errors
import config
import schema
import epta_pipeline_utils as epu


class Database(object):
    def __init__(self, autocommit=True, url=config.dburl, *args, **kwargs):
        """Set up a Toaster Database object using SQLAlchemy.
        """
        # Create the database engine
        self.engine = sa.create_engine(url, *args, **kwargs)
        sa.event.listen(self.engine, "before_cursor_execute", \
                            self.before_cursor_execute)
        self.autocommit = autocommit

        # The database description (metadata)
        self.metadata = schema.metadata
        self.tables = self.metadata.tables

    def connect(self):
        """Connect to the database, setting self.conn.
            
            Inputs:
                None

            Output:
                conn: The established SQLAlchemy Connection object, 
                    which is also available as self.conn.
        """
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
        if executemany:
            msg += "\n    Executing %d statements" % len(parameters)
        else:
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
        trans = self.open_transactions.pop()
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
        
