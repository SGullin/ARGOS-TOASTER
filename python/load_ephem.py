#!/usr/bin/env python2.6

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

import epta_pipeline_utils as epu

# Database configuration parameters
DB_HOST = "localhost"
DB_NAME = "epta"
DB_USER = "epta"
DB_PASS = "psr1937"


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
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
        description = 'Can be used with multiple par files, but place \
        filenames *before* the comments.')
    # 'files' will contain the par files to be uploaded:
    parser.add_argument( 'files',
                         nargs = '+',
                         type = str,
                         default = None,
                         help = "Parameter file to upload.")
    parser.add_argument( '-master',
                         action = 'store_true',
                         default = False,
                         help = 'Use if the par files should be master files\
                          for the respective pulsars.' )
    parser.add_argument( '--comments',
                         nargs = 1,
                         type = str,
                         default = None,
                         help = 'Provide comments describing the par files.\
                          These comments will be identical for all \
                         simultaneously uploaded templates. \
                         It should be a single quoted string.')
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

def Add_Parfile( file_path, file_name, checksum, DBcursor, args ):
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

    # Add comments
    if( args.comments ):
        QUERY = 'UPDATE parfiles SET comments = \'%s\' WHERE parfile_id = \
        \'%s\'' %( args.comments, par_id )
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

    PSR_Name = DBOUT[1]
    # Find pulsar ID:
    QUERY = 'select pulsar_name,count(*) from pulsars where pulsar_name = \'%s\'' %( PSR_Name )
    DBcursor.execute( QUERY )
    Have_Jfile = DBcursor.fetchall()[0][1]

    if( Have_Jfile == 0 ):
        print "WARNING: pulsar is not in the database\n"
        # Pulsar not in database yet.
        QUERY = "INSERT INTO pulsars SET pulsar_name = '%s'" % PSR_Name
        DBcursor.execute( QUERY )
        PSR_ID = DBcursor.execute( "SELECT LAST_INSERTED_ID()" )
        Make_master( PSR_ID, par_id )
    else:
        print "This pulsar is in the database\n"
        QUERY = 'select pulsar_id from pulsars where pulsar_name = \'%s\'' %(
            PSR_Name )
        DBcursor.execute( QUERY )
        # We'll select the first pulsar with this name (in case of duplication)
        PSR_ID = DBcursor.fetchall()[0][0]

    QUERY = 'UPDATE parfiles SET pulsar_id = \'%s\' \
    WHERE parfile_id = \'%s\'' %( PSR_ID, par_id )
    DBcursor.execute( QUERY )

    # Return parfile ID and PSRJ-name:
    return par_id, PSR_ID

def Make_master( PSR_ID, par_id ):
    QUERY = 'UPDATE pulsars SET master_parfile_id = \'%s\' \
    WHERE pulsar_id = \'%s\'' %( par_id, PSR_ID )
    DBcursor.execute( QUERY )

def Check_and_Load_Parfile( file_path, file_name, proc_id, args ):

    # Make DB connection
    DBcursor, DBconn = epu.DBconnect()

    # Determine md5 sum for given par file:
    checksum = epu.Get_md5sum(os.path.join(file_path, file_name))

    # Check for presence of checksum in database:
    QUERY = "select md5sum, count(*) from parfiles where md5sum = '%s';"%(
        checksum)
    DBcursor.execute( QUERY )
    Have_file = DBcursor.fetchall()[0][1]

    if( Have_file == 0 ):
        par_id, PSR_ID = Add_Parfile( file_path, file_name, checksum,
                                      DBcursor, args )
    else:
        print 'File %s already existant in database!\n'\
              %( os.path.join( file_path, file_name ) )
                 
    # Make this the master par file if requested or needed:
    if( args.master ):
        print "JORIS!"
        Make_master( PSR_ID, par_id )
    # THIS DOESN'T WORK YET BECAUSE THE PULSARS AND PARFILES TABLES AREN'T
    # LINKED YET.

    # Should we also make it the master if this is the first par
    # file loaded for a pulsar?

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
    if args.files:
        for file in args.files:
            # Check if par file exists:
            file_path, file_name = Verify_file_path( file, verbose = VERBOSE )
            if VERBOSE:
                print "Running on %s" %( os.path.join( file_path, file_name ) )
                
            # Load par file into the database:
            Check_and_Load_Parfile( file_path, file_name, proc_id, args )
    else:
        print 'ERROR! You did not specify a par file!'
        print 'Exiting...'
        exit( -1 )

main()
