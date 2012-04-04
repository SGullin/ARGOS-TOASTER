import MySQLdb
import MySQLdb.cursors

import errors
import config
import epta_pipeline_utils as epu

cursor_classes = {'dict': MySQLdb.cursors.DictCursor, \
                  'default': MySQLdb.cursors.Cursor}

class Database(object):
    """Database object for connecting to the EPTA database
        using MySQLdb
    """
    def __init__(self, cursor_class='default'):
        """Constructor for Database object.

            Inputs:
                cursor_class: A string referring to a cursor class.
                    'dict': MySQLdb.cursors.DictCursor,
                    'default: MySQLdb.cursors.Cursor.
                    (Default: 'default' - go figure!)

            Output:
                db: connected Database object.
        """
        self.cursor_class = cursor_classes[cursor_class]
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
                                        passwd=config.dbpass)
            self.cursor = self.conn.cursor(self.cursor_class)
            epu.print_debug("Successfully connected to database %s.%s as %s " \
                            "(cursor class: '%s')" % \
                        (config.dbhost, config.dbname, config.dbuser, \
                            self.cursor_class.__name__), \
                        'database')
        except MySQLdb.OperationalError:
            raise errors.DatabaseError("Could not connect to database!")

    def execute(self, query, *args, **kwargs):
        epu.print_debug(query, 'database')
        self.cursor.execute(query, *args, **kwargs)

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
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()
        
    def execute_and_fetchone(self, query, *args, **kwargs):
        self.execute(query, *args, **kwargs)
        return self.fetchone()

    def execute_and_fetchall(self, query, *args, **kwargs):
        self.execute(query, *args, **kwargs)
        return self.fetchall()

        
