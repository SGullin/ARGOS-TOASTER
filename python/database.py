import collections

import MySQLdb
import MySQLdb.cursors

import errors
import config
import epta_pipeline_utils as epu


class Database(object):
    """Database object for connecting to the EPTA database
        using MySQLdb
    """
    def __init__(self, autocommit=True):
        """Constructor for Database object.

            Inputs:
                autocommit: If True, automatically commit each query as 
                    it is executed. (Default: True)

            Output:
                db: connected Database object.
        """
        self.autocommit=autocommit
        self.connect()

    def connect(self):
        """Establish a database connection. Set self.conn and self.cursor.

            Inputs:
                None

            Output:
                None
        """
        try:
            self.conn = MySQLdb.connect(host=config.dbhost, \
                                        db=config.dbname, \
                                        user=config.dbuser, \
                                        passwd=config.dbpass, \
                                        autocommit=False)
            self.cursor = self.conn.cursor(MySQLdb.cursors.Cursor)
            epu.print_debug("Successfully connected to database %s.%s as %s" % \
                        (config.dbhost, config.dbname, config.dbuser), \
                        'database')
        except MySQLdb.OperationalError:
            raise errors.DatabaseError("Could not connect to database!")

    def execute(self, query, *args, **kwargs):
        msg = "Query: %s" % query
        if args:
            msg += "\nArgs: %s" % args
        if kwargs:
            msg += "\nKeyword args: %s" % kwargs
        epu.print_debug(msg, 'database')
        self.cursor.execute(query, *args, **kwargs)
        if self.autocommit:
            self.commit()
        try:
            colnames = [d[0] for d in self.cursor.description]
            self.RowClass = collections.namedtuple("RowClass", colnames)
            self.row_maker = self.RowClass._make
        except:
            # If we run into a problem default to a tuple
            self.RowClass = tuple
            self.row_maker = self.RowClass

    def commit(self):
        """Commit a transaction.
            
            Inputs:
                None

            Outputs:
                None
        """
        if not self.autocommit:
            epu.print_debug("Manually committing transaction.", 'database')
        self.conn.commit()

    def rollback(self):
        """Rollback an uncommitted a transaction.
            
            Inputs:
                None

            Outputs:
                None
        """
        epu.print_debug("Rolling back transaction.", 'database')
        self.conn.rollback()

    def close(self):
        """Close the DB connection.

            Inputs:
                None

            Outputs:
                None
        """
        self.conn.close()
        epu.print_debug("Connection to database has been closed", 'database')

    def fetchall(self):
        return [self.row_maker(row) for row in self.cursor.fetchall()]

    def fetchone(self):
        return self.row_maker(self.cursor.fetchone())
        
    def execute_and_fetchone(self, query, *args, **kwargs):
        self.execute(query, *args, **kwargs)
        return self.fetchone()

    def execute_and_fetchall(self, query, *args, **kwargs):
        self.execute(query, *args, **kwargs)
        return self.fetchall()

        
