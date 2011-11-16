#!/usr/bin/env python

#
# load_ephem.py: script to upload par files to the EPTA timing database.
#
PIPE_NAME = "load_ephem"
VERSION = 0.1

# Import modules
from sys import argv, exit
from os import system, popen
from MySQLdb import *
import os.path
import datetime
import argparse
import hashlib

# Database configuration parameters
DB_HOST = "localhost"
DB_NAME = "epta2" 
DB_USER = "epta"
DB_PASS = "mysqlaccess"

# Python version
PYTHON = "/usr/bin/python"

# Storage directories
interfile_path = "/home/epta/database/data/interfiles"

# Debugging flags
VERBOSE = 1 # Verbosity flag
TEST = 0 # Prints commands without running them.

# Functions
def Help():
    # Print basic description and usage
    print "\nThe EPTA Timing Pipeline - par-file upload script.\n"
    print "Version: %.2f" %( VERSION ) + "\n"
    print "Please use 'load_ephem.py -h' for a full list of command-line"
    print " options\n"
    exit( 0 )

def Parse_command_line():
    parser = argparse.ArgumentParser(
        prog = 'load_ephem',
        formatter_class = argparse.ArgumentDefaultsHelpFormatter )
    # 'files' will contain the par files to be uploaded:
    parser.add_argument( 'files',
                         nargs = '+',
                         type = str,
                         default = None,
                         help = "Parameter file to upload.")
    parser.add_argument( '--master', '-m',
                         nargs = 1,
                         type = int,
                         default = 0,
                         help = "Whether the new par file(s) should (1) or \
                         should not (0) be a master file for the \
                         respective pulsars." )
    args = parser.parse_args()
    return args

def Verify_file_path( file, verbose = 0 ):
    # Check that file exists:
    if not os.path.isfile( file ):
        print "ERROR: File %s does not exist!" %( file )
        exit( 0 )
    elif os.path.isfile( file ) and verbose:
        print "File %s exists!" %( file )

    # Determine path (will retrieve absolute path)
    file_path, file_name = os.path.split( os.path.abspath( file ) )
    if verbose:
        print "Path: %s; Filename: %s" %( file_path, file_name )
    return file_path, file_name

def Make_Proc_ID():
    utcnow = datetime.datetime.utcnow()
    return "%d%02d%02d_%02d%02d%02d.%d" %( utcnow.year, utcnow.month,
                                           utcnow.day, utcnow.hour,
                                           utcnow.minute, utcnow.second,
                                           utcnow.microsecond )

def Get_Checksum( file ):
    # Returns the MD5 checksum for the input file
    m = hashlib.md5()
    # open the file
    f = open( file, 'r' )
    # Read contents of file
    m.update( f.read() )
    # Close file
    f.close()
    # Determine the checksum
    checksum = m.hexdigest()
    return checksum

def DBconnect( Host, DBname, Username, Password ):
    # To make a connection to the database
    try:
        connection = connect( host = Host,
                              db = DBname,
                              user = Username,
                              passwd = Password )
        cursor = connection.cursor()
        print "Successfully connected to database %s.%s as %s" %( Host,
                                                                  DBname,
                                                                  Username )
    except OperationalError:
        print "ERROR! Could not connect to database! Exiting..."
        exit( 0 )
    return cursor, connection

def Parse_parfile( file ):
    # Parses the parfile info from the psrfits file
    parfile_names = []
    parfile_values = []
    lines = open( file, 'r' ).readlines()
    for line in lines[0:]:
        line_split = line.split()
        if len( line_split ) > 0:
            parfile_names.append( line_split[0].strip() )
            parfile_values.append( line_split[1].strip() )

    return zip( parfile_names, parfile_values )

def Add_Parfile( file_path, file_name, checksum, DBcursor ):
    if VERBOSE:
        print "Importing parfile information for %s" %(
            os.path.join( file_path, file_name ) )
    # Set time of par-file addition:
    QUERY = "INSERT INTO parfiles SET add_time = '%s'" %(
        datetime.datetime.utcnow() )
    DBcursor.execute( QUERY )

    # Get parfile ID:
    QUERY = "SELECT LAST_INSERT_ID()"
    DBcursor.execute( QUERY )
    par_id = DBcursor.fetchall()[0][0]

    # Add filename and path to parfiles table:
    QUERY = "UPDATE parfiles SET filename = '%s' WHERE parfile_id = '%s'" %(
        file_name, par_id )
    DBcursor.execute( QUERY )
    QUERY = "UPDATE parfiles SET filepath = '%s' WHERE parfile_id = '%s'" %(
        file_path, par_id )
    DBcursor.execute( QUERY )

    # Add Checksum:
    QUERY = "UPDATE parfiles SET md5sum = '%s' WHERE parfile_id = '%s'" %(
        checksum, par_id )
    DBcursor.execute( QUERY )

    # Now add all parfile parameters:
    for param in Parse_parfile( os.path.join( file_path, file_name ) ):
        if param[0] == 'BINARY':
            Corrected_Param = 'BINARY_MODEL'
        else:
            Corrected_Param = param[0]
        QUERY = "UPDATE parfiles SET %s = '%s' WHERE parfile_id = '%s'" %(
            Corrected_Param, param[1], par_id )
        try:
            DBcursor.execute( QUERY )
        except( OperationalError, ProgrammingError ):
            if VERBOSE:
                print "WARNING! No parameter corresponding to %s" %( param[0] )

    QUERY = "SELECT parfile_id, PSRJ, F0, DM FROM parfiles\
    WHERE parfile_id = '%s'" %( par_id )
    DBcursor.execute( QUERY )
    DBOUT = DBcursor.fetchall()[0]
    par_id = DBOUT[0]
    if VERBOSE:
        print "par_id\tPSRJ\tFO\tDM"
        print "\t".join( "%s" %val for val in DBOUT[0:4] )

    # Return parfile ID and PSRJ-name:
    return par_id, DBOUT[1]

def Check_and_Load_Parfile( file_path, file_name, proc_id, args ):

    # Make DB connection
    DBcursor, DBconn = DBconnect( DB_HOST, DB_NAME, DB_USER, DB_PASS )

    # Determine md5 sum for given par file:
    checksum = Get_Checksum( os.path.join( file_path, file_name ) )

    # Check for presence of checksum in database:
    QUERY = "select md5sum, count(*) from parfiles where md5sum = '%s';"%(
        checksum)
    # Next line for testing only.
    #QUERY = "select PSRJ, count(*) from parfiles where PSRJ = 'J1713+074';"
    DBcursor.execute( QUERY )
    # fetchall()[0][0] is 'md5sum';
    # fetchall()[0][1] is the number of lines in the db with the given md5sum.
    Have_file = DBcursor.fetchall()[0][1]

    if VERBOSE:
        print "Result: %s" %( Have_file )
        
    if( Have_file == 0 ):
        par_id, PSRJ = Add_Parfile( file_path, file_name, checksum, DBcursor )

    # Make this the master par file if requested or needed:
    #if args.master:
    #    Make_master( par_id )
    # THIS DOESN'T WORK YET BECAUSE THE PULSARS AND PARFILES TABLES AREN'T
    # LINKED YET.

    # Close the DB connection
    DBconn.close()

def main():

    # Provide help if no command-line arguments are given.
    if len( argv ) < 2:
        Help()

    # Determine the command-line arguments
    args = Parse_command_line( )

    # Get process ID:
    proc_id = Make_Proc_ID( )

    # Now load all par files into database:
    for file in args.files:
        # Check if par file exists:
        file_path, file_name = Verify_file_path( file, verbose = VERBOSE )
        if VERBOSE:
            print "Running on %s" %( os.path.join( file_path, file_name ) )
            
        # Load par file into the database:
        Check_and_Load_Parfile( file_path, file_name, proc_id, args )

        # Make the file the master file if needed
        #if args.master:
        #    Make_master( file_path, file_name )

        # Should we also make it the master if this is the first par
        # file loaded for a pulsar?

main()
