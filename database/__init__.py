import warnings
import string
import re

import sqlalchemy as sa

from toaster import config
from toaster import errors
from toaster import debug

from toaster.database import schema
from toaster.utils import notify

null = lambda x: x

toround_re = re.compile(r"_R(-?\d+)?$")


def fancy_getitem(self, key):
    filterfunc = null
    if (type(key) in (type('str'), type(u'str'))) and key.endswith("_L"):
        filterfunc = string.lower
        key = key[:-2]
    elif (type(key) in (type('str'), type(u'str'))) and key.endswith("_U"):
        filterfunc = string.upper
        key = key[:-2]
    elif (type(key) in (type('str'), type(u'str'))) and toround_re.search(key):
        head, sep, tail = key.rpartition('_R')
        digits = int(tail) if tail else 0
        filterfunc = lambda x: round(x, digits)
        key = head
    if key in self:
        return filterfunc(super(self.__class__, self).__getitem__(key))
    else:
        matches = [k for k in self.keys() if k.startswith(key)]
        if len(matches) == 1:
            return filterfunc(super(self.__class__, self).__getitem__(matches[0]))
        elif len(matches) > 1:
            raise errors.BadColumnNameError("The column abbreviation "
                                            "'%s' is ambiguous. "
                                            "('%s' all match)" %
                                            (key, "', '".join(matches)))
        else:
            raise errors.BadColumnNameError("The column '%s' doesn't exist! "
                                            "(Valid column names: '%s')" %
                                            (key, "', '".join(sorted(self.keys()))))

sa.engine.RowProxy.__getitem__ = fancy_getitem
    

def before_cursor_execute(conn, cursor, statement, parameters,
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
    notify.print_debug(msg, "queries", stepsback=7)


def commit_event(conn):
    """An event to be executed when a transaction is committed.

        See SQLAlchemy for details about event triggers.
    """
    notify.print_debug("Committing database transaction.", 'database',
                       stepsback=7)


def rollback_event(conn):
    """An event to be executed when a transaction is rolled back.
        
        See SQLAlchemy for details about event triggers.
    """
    notify.print_debug("Rolling back database transaction.", 'database',
                       stepsback=7)
        

def begin_event(conn):
    """An event to be executed when a transaction is opened.
        
        See SQLAlchemy for details about event triggers.
    """
    notify.print_debug("Opening database transaction.", 'database',
                       stepsback=7)


# Cache of database engines
engines = {}


def get_toaster_engine(url=None):
    """Given a DB URL string return the corresponding DB engine.
        Create the Engine object if necessary. If the engine 
        already exists return it rather than creating a new one.

        Input:
            url: A DB URL string.

        Output:
            engine: The corresponding DB engine.
    """
    global engines
    if url is None:
        url = config.cfg.dburl
    if url not in engines:
        # Create the database engine
        engine = sa.create_engine(config.cfg.dburl)
        sa.event.listen(engine, "before_cursor_execute",
                        before_cursor_execute)
        if debug.is_on('database'):
            sa.event.listen(engine, "commit", commit_event)
            sa.event.listen(engine, "rollback", rollback_event)
            sa.event.listen(engine, "begin", begin_event)
        engines[url] = engine
    return engines[url]


class Database(object):
    def __init__(self, autocommit=True):
        """Set up a Toaster Database object using SQLAlchemy.
        """
        self.conn = None  # No connection is established
                          # until self.connect() is called
        self.engine = get_toaster_engine()
        if not self.is_created():
            raise errors.DatabaseError("The database (%s) does not appear "
                                       "to have any tables. Be sure to run "
                                       "'create_tables.py' before attempting "
                                       "to connect to the database." %
                                       self.engine.url.database)
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
        conn = self.engine.connect()
        table_names = self.engine.table_names(connection=conn)
        conn.close()
        return bool(table_names)

    def connect(self):
        """Connect to the database, setting self.conn.
            
            Inputs:
                None

            Output:
                conn: The established SQLAlchemy Connection object, 
                    which is also available as self.conn.
        """
        # Only open a connection if not already connected
        if not self.is_connected():
            # Establish a connection
            self.conn = self.engine.connect()
            self.conn.execution_options(autocommit=self.autocommit)
            self.open_transactions = []
            self.result = None
            if self.engine.dialect.name == 'sqlite':
                result = self.execute("PRAGMA foreign_keys=ON")
                result.close()
            notify.print_debug("Database connection established.",
                               'database',
                               stepsback=2)
        return self.conn

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
            raise errors.DatabaseError("Connection to database not "
                                       "established. Be sure "
                                       "self.connect(...) is called "
                                       "before attempting to execute "
                                       "queries.")
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
        notify.print_debug("Attempting to begin a transaction via "
                           "database object", 'database', stepsback=2)
        if not self.is_connected():
            raise errors.DatabaseError("Connection to database not "
                                       "established. Be sure "
                                       "self.connect(...) is called "
                                       "before attempting to execute "
                                       "queries.")
        if self.open_transactions:
            warnings.warn("A transaction already appears to be in progress.",
                          errors.ToasterWarning)
        trans = self.conn.begin()
        self.open_transactions.append(trans)
        return trans

    def commit(self):
        """Commit the most recently opened transaction.
            
            Inputs:
                None

            Outputs:
                None
        """
        notify.print_debug("Attempting to commit a transaction via "
                           "database object", 'database', stepsback=2)
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
        notify.print_debug("Attempting to roll back a transaction via "
                           "database object", 'database', stepsback=2)
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
            notify.print_debug("Database connection closed.", 'database',
                               stepsback=2)
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

    def execute_and_fetchall(self, *args, **kwargs):
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
