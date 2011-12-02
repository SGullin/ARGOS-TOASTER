#!/usr/bin/env python2.6
####################
#
# load_template.py: Script to upload template profiles to the EPTA
#                   timing database
#
SCRIPT_NAME = "load_template"
VERSION = 0.1
####################

#Imported modules
from sys import argv, exit
from os import system, popen
from MySQLdb import *
import os.path
import datetime
import argparse
import hashlib
from subprocess import *

##############################################################################
# CONFIG PARAMS
##############################################################################

#Database configuration parameters
DB_HOST = "localhost"
DB_NAME = "epta2"
DB_USER = "epta"
DB_PASS = "mysqlaccess"

#Debugging flags
VERBOSE = 1 #Print extra output
TEST = 0 #Prints commands and other actions without running them

#Functions
def Help():
    #Print basic description and usage
    print "\nPython script to upload templates"
    print "Version: %.2f"%(VERSION)+"\n"
        
    print "Please use '%s"%(SCRIPT_NAME)+".py -h' for a full list of command \
line options. \n"
    exit(0)

def Parse_command_line():
    parser = argparse.ArgumentParser(
        prog=SCRIPT_NAME+'.py',
        description="Either use with multiple psrfits standards, or a single \
        .m file. Place template filenames *before* comments.")
    parser.add_argument('--pulsar',
                        nargs=1,
                        type=str,
                        default=None,
                        help="Pulsar name (if not specified in template \
                        header).")
    parser.add_argument('--system',
                        nargs=1,
                        type=str,
                        default=None,
                        help="Observing system (if not specified in template \
                        header).")
    parser.add_argument( '-master',
                         action = 'store_true',
                         default = False,
                         help = "Whether or not the provided file(s) are to be\
                          master templates.")
    parser.add_argument('--comments',
                        nargs='+',
                        type=str,
                        default=None,
                        help="Provide comments describing the template(s). \
                        These comments will be identical for all templates.")
    parser.add_argument('templates',
                        nargs='+',
                        type=str,
                        default=None,
                        help="File names of the templates to upload.")
    args=parser.parse_args()
    return args

def DBconnect(Host,DBname,Username,Password):
    #To make a connection to the database
    try:
        connection = connect(host=Host,db=DBname,user=Username,passwd=Password)
        cursor = connection.cursor()
        print "Successfully connected to database %s.%s as %s"%(
            Host,DBname,Username)
    except OperationalError:
        print "Could not connect to database!  Exiting..."
        exit(0)
    return cursor, connection
                    
def Verify_file_path(file, verbose=0):
    #Verify that file exists
    if not os.path.isfile( file ):
        print "ERROR! File %s does not exist!"%( file )
        exit(0)
    elif  os.path.isfile( file ) and verbose:
        print "File %s exists!"%(file)
        
    #Determine path (will retrieve absolute path)
    file_path, file_name = os.path.split( os.path.abspath( file ) )
    if verbose:
        print "Path: %s; Filename: %s"%(file_path, file_name)
    return file_path, file_name

def Make_Proc_ID():
    utcnow = datetime.datetime.utcnow()
    return "%d%02d%02d_%02d%02d%02d.%d"%(utcnow.year,utcnow.month,
                                         utcnow.day,utcnow.hour,
                                         utcnow.minute,utcnow.second,
                                         utcnow.microsecond)

def Give_UTC_now():
    utcnow = datetime.datetime.utcnow()
    return "UTC %d:%02d:%02d on %d%02d%02d"%(utcnow.hour,utcnow.minute,
                                             utcnow.second,utcnow.year,
                                             utcnow.month,utcnow.day)

def Get_Checksum( filename ):
    # Returns the MD5 checksum for the input file
    m = hashlib.md5( )
    # open the file
    f = open( file, 'r' )
    # Read contents of file
    m.update( f.read( ) )
    # Close file
    f.close( )
    # Determine the checksum
    checksum = m.hexdigest( )
    return checksum

def Get_Pulsar_ID( name, DBcursor ):
    # Gets the pulsar ID from the database, regardless of whether the
    # given name is a B-name or a J-name.
    QUERY = "select PSRJ, count(*) from pulsars where PSRJ = '%s'" %( name )
    DBcursor.execute( QUERY )
    Have_Jfile = DBcursor.fetchall()[0][1]

    if( Have_Jfile == 0 ):
        # No J-name. Trying B-name
        QUERY = "select PSRB, count(*) from pulsars where PSRB = '%s'" %(
            name )
        DBcursor.execute( QUERY )
        Have_Bfile = DBcursor.fetchall()[0][1]
        # Now get pulsar id:
        if( Have_Bfile != 0 ):
            QUERY = "select pulsar_id from pulsars where PSRB = '%s'" %( name )
            DBcursor.execute( QUERY )
            PSR_ID = DBcursor.fetchall()[0][0]
    else:
        # Have J-name.
        QUERY = "select pulsar_id from pulsars where PSRJ = '%s'" %( name )
        DBcursor.execute( QUERY )

        # Select the first occurrence of this pulsar:
        # (There should only be one)
        PSR_ID = DBcursor.fetchall()[0][0]

    if( Have_Bfile == 0 & Have_Jfile == 0 ):
        # New pulsar
        if name[0] == 'B':
            Have_Bname = 1
            QUERY = "INSERT INTO pulsars SET PSRB = '%s'" %( name )
        elif name[0] == 'J':
            Have_Jname = 1
            QUERY = "INSERT INTO pulsars set PSRJ = '%s'" %( name )
        else:
            print 'Don\'t know if pulsar name (%s) is a J or a B-name. \
            Exiting...' %( name )
            exit( 1 )
        DBcursor.execute( QUERY )
        PSR_ID = DBcursor.execute( "SELECT LAST_INSERT_ID()" )

    return PSR_ID

def Add_Template( file_path, file_name, checksum, DBcursor, args ):
    if VERBOSE:
        print "Importing template file %s...\n" %(
            os.path.join( file_path, file_name ) )

    # Set time of template addition:
    QUERY = "INSERT INTO templates SET add_time = '%s'" %(
        datetime.datetime.utcnow( ) )
    DBcursor.execute( QUERY )

    # Get template ID:
    QUERY = "SELECT LAST_INSERT_ID()"
    DBcursor.execute( QUERY )
    template_id = DBcursor.fetchall()[0][0]

    # Add file name, file path and checksum:
    QUERY = "UPDATE templates SET filename = '%s' WHERE template_id = '%s'" %(
        file_name, template_id )
    DBcursor.execute( QUERY )
    QUERY = "UPDATE templates SET filepath = '%s' WHERE template_id = '%s'" %(
        file_path, template_id )
    DBcursor.execute( QUERY )
    QUERY = "UPDATE templates SET md5sum = '%s' WHERE template_id = '%s'" %(
        checksum, template_id )
    DBcursor.execute( QUERY )

    # Add comments
    QUERY = "UPDATE templates SET comments = '%s' WHERE template_id = '%s'" %(
        args.comments, template_id )
    DBcursor.execute( QUERY )
    
    # Now figure out if this is an analytic template or psrfits format:
    p = Popen( "vap -c LENGTH %s"%( os.path.join( file_path, file_name ) ),
               shell = False, bufsize = 4096, stdin = PIPE, stdout = PIPE,
               universal_newlines = False )
    Length = p.communicate()[0]
    if Length == 0:
        # Have analytic template
        QUERY = 'UPDATE templates SET is_analytic = 1 WHERE template_id\
        = \'%s\'' %( template_id )
        DBcursor.execute( QUERY )
        # Default the number of bins to 0 (because this value is irrelevant for
        # analytic templates) 
        QUERY = 'UPDATE templates SET nbin = 0 WHERE template_id = \'%s\'' %(
            template_id )
        DBcursor.execute( QUERY )
        # Set pulsar name
        if( args.pulsar ):
            PSR_ID = Get_Pulsar_ID( args.pulsar[0], DBcursor )
            QUERY = 'UPDATE templates SET pulsar_id = %s WHERE template_id\
            = \'%s\'' %( PSR_ID, template_id )
            DBcursor.execute( QUERY )
        else:
            print 'Specified an analytic template but did not specify\
             the pulsar name.'
            print 'Exiting...\n"'
            exit( -1 )

    else:
        # Have a psrchive-format file
        QUERY = 'UPDATE templates SET is_analytic = 0 WHERE template_id\
        = \'%s\'' %( template_id )
        DBcursor.execute( QUERY )
        # Set nbins:
        p = Popen( "vap -c NBIN %s" %( os.path.join( file_path, file_name ) ),
                   shell = False, bufsize = 4096, stdin = PIPE, stdout = PIPE,
                   universal_newlines = False )
        Nbins = p.communicate()[0].split()[3]
        QUERY = 'UPDATE templates SET nbin = %d WHERE template_id = \'%s\''\
                %( Nbins, template_id )
        DBcursor.execute( QUERY )
        # Set pulsar name:
        p = Popen( "vap -c NAME %s" %( os.path.join( file_path, file_name ) ),
                   shell = False, bufsize = 4096, stdin = PIPE, stdout = PIPE,
                   universal_newlines = False )
        PSRName = p.communicate()[0].split()[3]
        PSR_ID = Get_Pulsar_ID( PSRName, DBcursor )
        QUERY = 'UPDATE templates SET pulsar_id = %s WHERE template_id\
         = \'%s\'' %( PSR_ID, template_id )
        DBcursor.execute( QUERY )

    # Identify User
    p = Popen( 'whoami', stdin = PIPE, stdout = PIPE )
    USRname = p.communicate()[0].split()[0]
    QUERY = 'select user_id, count(*) from users where user_name\
    = \'%s\'' %( USRname )
    DBcursor.execute( QUERY )
    Have_user = DBcursor.fetchall()[0][1]

    if( Have_user == 0 ):
        print "User unknown! Don't know what to do next..."
        print "Exiting..."
        exit( -1 )
    elif( Have_user > 1 ):
        print "Multiple users with name >>%s<<." %( USRname )
        print "Please fix the database."
        print "Exiting..."
        exit( -1 )
    else:
        # Have a unique user
        QUERY = 'select user_id from users where user_name = \'%s\''\
                %( USRname )
        DBcursor.execute( QUERY )
        USR_ID = DBcursor.fetchall()[0][0]
        QUERY = 'UPDATE templates SET user_id = \'%s\' WHERE template_id\
        = \'%s\'' %( USR_ID, template_id )
        DBcursor.execute( QUERY )

    # Determine observing system based on backend name
    if( args.system ):
        # System defined on command line
        QUERY = 'select obssystem_id, count (*) from obssystems where name =\
        \'%s\'' %( args.system[0] )
        DBcursor.execute( QUERY )
        Have_backend = DBcursor.fetchall()[0][1]
        if( Have_backend == 1 ):
            # Have a unique observing system
            QUERY = 'select obssystem_id from obssystems where name = \'%s\''\
                    %( args.system[0] )
            DBcursor.execute( QUERY )
            BE_ID = DBcursor.fetchall()[0][0]
        elif( Have_backend == 0 ):
            print 'No observing system named >>%s<< known.' %( args.system[0] )
            print 'Exiting...'
            exit( -1 )
        else:
            print 'Multiple observing systems named >>%s<<.' %(
                args.system[0] )
            print 'Please fix database.\n'
            print 'Exiting...'
            exit( -1 )
    else:
        # System is not defined on the command line:
        p = Popen( 'vap -c backend %s' %( \
            os.path.join( file_path, file_name ) ),
                   shell = False, stdin = PIPE, stdout = PIPE )
        Backend = p.communicate()[0].split()[3]
        QUERY = 'select obssystem_id, count (*) from obssystems where name = \
        \'%s\'' %( Backend )
        DBcursor.execute( QUERY )
        Have_backend = DBcursor.fetchall()[0][1]
        if( Have_backend == 1 ):
            QUERY = 'select obssystem_id from obssystems where name = \
            \'%s\'' %( Backend )
            DBcursor.execute( QUERY )
            BE_ID = DBcursor.fetchall()[0][0]
        elif( Have_backend == 0 ):
            print "Observing system (%s) not yet known." %( Backend )
            print "Exiting..."
            exit( -1 )
        else:
            print "Multiple observing systems with backend %s." %( Backend )
            print "Trying to find unique system..."
            p = Popen( 'vap -c telescop %s'\
                       %( os.path.join( file_path, file_name ) ),
                       shell = False, stdin = PIPE, stdout = PIPE )
            Telescope = p.communicate()[0].split()[3]
            # Now get the telescope ID:
            QUERY = 'select telescope_id, count (*) from telescopes where\
            name = \'%s\'' %( Telescope )
            DBcursor.execute( QUERY )
            Have_Telescope = DBcursor.fetchall()[0][1]
            if( Have_Telescope == 0 ):
                print 'Don\'t know telescope (%s).' %( Telescope )
                print 'Exiting...'
                exit( -1 )
            elif( Have_Telescope > 1 ):
                print 'Multiple entries for telescope (%s).' %( Telescope )
                print 'Please fix database.'
                print 'Exiting...'
                exit( -1 )
            else:
                QUERY = 'select telescope_id from telescopes where name =\
                \'%s\'' %( Telescope )
                DBcursor.execute( QUERY )
                Tel_ID = DBcursor.fetchall()[0][0]
                QUERY = 'select obssystem_id, count (*) from obssystems\
                where ( ( name = \'%s\' ) and ( telescope_id = \'%s\' ) )'\
                %( Backend, Tel_ID )
                DBcursor.execute( QUERY )
                Have_backend = DBcursor.fetchall()[0][1]
                if( Have_backend == 1 ):
                    QUERY = 'select obssystem_id from obssystems where \
                    ( ( name = \'%s\' ) and ( telescope_id = \'%s\' ) )'\
                    %( Backend, Tel_ID )
                    DBcursor.execute( QUERY )
                    BE_ID = DBcursor.fetchall()[0][0]
                elif( Have_backend == 0 ):
                    print 'No match for backend >>%s<< and \
                    telescope >>%s<<.' %( Backend, Telescope )
                    print 'Exiting...'
                    exit( -1 )
                else:
                    print 'Multiple observing systems with backend (%s) \
                    and telescope (%s). Please fix database.' %( Backend,
                                                                 Telescope )
                    print 'Exiting...'
                    exit( -1 )
                        
    QUERY = 'UPDATE templates SET obssystem_id = \'%s\' WHERE \
    template_id = \'%s\'' %( BE_ID, template_id )
    DBcursor.execute( QUERY )

    # Now figure out if this is a master template.
    if( args.master ):
        # Defined as master.
        # Now enter this information into the master_templates table
        QUERY  = 'INSERT INTO master_templates SET template_id = \'%s\'\
        , pulsar_id = \'%s\', obssystem_id = \'%s\'' %( template_id, PSR_ID,
                                                        BE_ID )
        DBcursor.execute( QUERY )
        
    
        

def Check_and_Load_Template( file_path, file_name, args ):

    # Make DB connection
    DBcursor, DBconn = DBconnect( DB_HOST, DB_NAME, DB_USER, DB_PASS )

    # Determine md5 checksum for given par file:
    checksum = Get_Checksum( os.path.join( file_path, file_name ) )

    # Check for presence of checksum in database:
    QUERY = "select md5sum, count(*) from templates where md5sum = '%s';" %(
        checksum )
    DBcursor.execute( QUERY )
    Have_file = DBcursor.fetchall()[0][1]

    if VERBOSE:
        print "New file: %s" %( Have_file )

    if( Have_file == 0 ):
        template_id = Add_Template( file_path, file_name, checksum, DBcursor,
                                    args )
    else:
        print "File %s already exists in the database!\n"\
              %( os.path.join( file_path, file_name ) )

    # Close the DB connection:
    DBconn.close( )

def main():

    # Provide help if no command-line arguments are given:
    if len( argv ) < 2:
        Help( )

    # Determine the command-line arguments:
    args = Parse_command_line( )

    # Get process ID:
    # proc_id = Make_Proc_ID( )
    # Don't need a process ID because processes are only things that create
    # TOAs. Uploading a template profile is not a process.

    # Now load all template files into database:
    if args.templates:
        for file in args.templates:
            # Check if template exists:
            file_path, file_name = Verify_file_path( file, verbose = VERBOSE )
            if VERBOSE:
                print "Running on %s" %( os.path.join( file_path, file_name ) )
            Check_and_Load_Template( file_path, file_name, args )

    else:
        print '\nERROR! You haven\'t specified a valid set of command line \
        options.  Exiting...'
        Help()
        
main()
