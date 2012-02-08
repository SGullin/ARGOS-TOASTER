#!/usr/bin/python2.6
################################
# epta_pipeline_utils.py    
# Useful, general functions 
################################

#Imported modules

from sys import argv, exit
from os import system, popen
from MySQLdb import *
import os.path
import datetime
import argparse
import hashlib

##############################################################################
# CONFIG PARAMS
#############################################################################

PIPE_NAME = "epta_pipeline"
VERSION = 0.1

#Database parameters
DB_HOST = "eptadata.jb.man.ac.uk"
DB_NAME = "epta"
DB_USER = "epta"
DB_PASS = "psr1937"

#Python version to use
PYTHON = "/usr/bin/python"

#Storage directories
interfile_path="/home/epta/database/data/interfiles"

#Debugging flags
VERBOSE = 1 #Print extra output
TEST = 0 #Prints commands and other actions without running them

##############################################################################
# Functions
##############################################################################

def DBconnect(Host=DB_HOST,DBname=DB_NAME,Username=DB_USER,Password=DB_PASS, \
                cursor_class=cursors.Cursor):
    #To make a connection to the database
    try:
        connection = connect(host=Host,db=DBname,user=Username,passwd=Password)
        cursor = connection.cursor(cursor_class)
        print "Successfully connected to database %s.%s as %s"%(Host,DBname,Username)
    except OperationalError:
        print "Could not connect to database!  Exiting..."
        exit(0)
    return cursor, connection
                    
def Run_python_script(script, args_list, verbose=0, test=0):
    #Use to run an external python script in the shell
    COMMAND = PYTHON+" "+script+" "+" ".join("%s" % arg for arg in args_list)
    if verbose:
        print "Running command: "+COMMAND
    if not test:
        system(COMMAND)

def Run_shell_command(command, verbose=0, test=0):
    #Use to run an external program in the shell
    COMMAND = command
    if verbose:
        print "Running command: "+COMMAND
    if not test:
        system(COMMAND)        

def Verify_file_path(file, verbose=0):
    #Verify that file exists
    if not os.path.isfile(file):
        print "File %s does not exist, you dumb dummy!"%(file)
        exit(0)
    elif  os.path.isfile(file) and verbose:
        print "File %s exists!"%(file)
    #Determine path (will retrieve absolute path)
    file_path, file_name = os.path.split(os.path.abspath(file))
    if verbose:
        print "Path: %s Filename: %s"%(file_path, file_name)
    return file_path, file_name

def Fill_pipeline_table(DBcursor,DBconn):
    #Calculate md5sum of pipeline script
    MD5SUM = popen("md5sum %s"%argv[0],"r").readline().split()[0].strip()
    QUERY = "INSERT INTO pipeline (pipeline_name, pipeline_version, md5sum) VALUES ('%s','%s','%s')"%(PIPE_NAME,VERSION,MD5SUM)
    DBcursor.execute(QUERY)
    #Get pipeline_id
    QUERY = "SELECT LAST_INSERT_ID()"
    DBcursor.execute(QUERY)
    pipeline_id = DBcursor.fetchall()[0][0]
    print "Added pipeline name and version to pipeline table with pipeline_id = %s"%pipeline_id
    return pipeline_id
    
def Make_Proc_ID():
    utcnow = datetime.datetime.utcnow()
    return "%d%02d%02d_%02d%02d%02d.%d"%(utcnow.year,utcnow.month,utcnow.day,utcnow.hour,utcnow.minute,utcnow.second,utcnow.microsecond)

def Make_Tstamp():
        utcnow = datetime.datetime.utcnow()
        return "%04d-%02d-%02d %02d:%02d:%02d"%(utcnow.year,utcnow.month,utcnow.day,utcnow.hour,utcnow.minute,utcnow.second)

def Give_UTC_now():
    utcnow = datetime.datetime.utcnow()
    return "UTC %d:%02d:%02d on %d%02d%02d"%(utcnow.hour,utcnow.minute,utcnow.second,utcnow.year,utcnow.month,utcnow.day)

def Get_md5sum(fname, block_size=16*8192):
    """Compute and return the MD5 sum for the given file.
        The file is read in blocks of 'block_size' bytes.

        Inputs:
            fname: The name of the file to get the md5 for.
            block_size: The number of bytes to read at a time.
                (Default: 16*8192)

        Output:
            md5: The hexidecimal string of the MD5 checksum.
    """
    f = open(fname, 'rb')
    md5 = hashlib.md5()
    block = f.read(block_size)
    while block:
        md5.update(block)
        block = f.read(block_size)
    f.close()
    return md5.hexdigest()
                                        
