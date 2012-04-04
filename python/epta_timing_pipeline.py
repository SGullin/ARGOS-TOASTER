#!/usr/bin/python
##################################
# epta_timing_pipeline.py 
PIPE_NAME = "epta_timing_pipeline"
VERSION = 0.2
##################################

#Imported modules
from sys import argv, exit
from os import system, popen
from MySQLdb import *
import os.path
import datetime
import argparse
import epta_pipeline_utils as epta

##############################################################################
# CONFIG PARAMS
##############################################################################

#Database parameters
DB_HOST = "localhost"
DB_NAME = "epta"
DB_USER = "epta"
DB_PASS = "psr1937"

#Python version to use
PYTHON = "/usr/bin/python"

#Archive base path storage directories
interfile_path="/home/epta/database/data/"

#Debugging flags
VERBOSE = 1 #Print extra output
TEST = 0 #Prints commands and other actions without running them

###############################################################################
# DO NOT EDIT BELOW HERE
###############################################################################

#Functions
def Help():
    #Print basic description and usage
    print "\nThe EPTA Timing Pipeline"
    print "Version: %.2f"%(VERSION)+"\n"

    print "To run a test on J1713+0747 use rawfile_id=80 parfile_id=15 and template_id=1"
    print "i.e. run './epta_timing_pipeline.py --rawfile_id 80 --parfile_id 15 --template_id 1'"
        
    print "Please use 'epta_timing_pipeline.py -h' for a full list of command line options. \n"

    print "\n"
    exit(0)

def Parse_command_line():
    parser = argparse.ArgumentParser(
        prog='epta_pipeline',
        description='')
    #Raw data
    parser.add_argument('--rawfile_id',
                        nargs=1,
                        type=int,
                        default=None,
                        help="ID of raw data file to use for running the full pipeline.")
    #Ephemeris
    parser.add_argument('--parfile_id',
                        nargs=1,
                        type=int,
                        default=None,
                        help="ID of ephemeris to use for running the full pipeline.")
    #Template profile
    parser.add_argument('--template_id',
                        nargs=1,
                        type=int,
                        default=None,
                        help="ID of template profile to use for running the full pipeline.")
    args=parser.parse_args()
    return args


def main():

    #Exit if there are no or insufficient arguments
    if len(argv) < 2:
        Help()

    args = Parse_command_line()

    if not (args.rawfile_id and args.parfile_id and args.template_id):
        print "\nYou haven't specified a valid set of command line options.  Exiting..."
        Help()

    rawfile_id = args.rawfile_id[0]

    #Start pipeline
    print "###################################################"
    print "Starting EPTA Timing Pipeline Version %.2f"%VERSION
    proc_id = epta.Make_Proc_ID()
    print "Proc ID (UTC start datetime): %s"%proc_id
    print "Start time: %s"%epta.Give_UTC_now()
    print "###################################################"

    #Make DB connection
    DBcursor, DBconn = epta.DBconnect(DB_HOST,DB_NAME,DB_USER,DB_PASS)

    DBcursor.execute("show tables")
    print DBcursor.fetchall()

    #Fill pipeline table
    #pipeline_id = epta.Fill_pipeline_table(DBcursor,DBconn)

    #Get raw data from rawfile_id and verify MD5SUM
    #raw_file = epta.get_raw_data()
    print rawfile_id
    DBcursor.execute("select filename, filepath, md5sum from rawfiles where rawfile_id = %d"%rawfile_id)
    filename, filepath, md5sum_DB = DBcursor.fetchall()[0]
    rawfile = os.path.join(filepath,filename)
    epta.Verify_file_path(rawfile)
    md5sum_file = epta.Get_md5sum(rawfile)
    if md5sum_DB == md5sum_file:
        print "md5sum check succeeded"
        return 0
    else:
        print "md5sum check failed"
        return 1
        
    #Get ephemeris from ephem_id and verify MD5SUM
    #ephemeris = epta.get_ephem()

    #Reinstall ephemeris with pam

    #Scrunch data in time/freq

    #Make diagnostic plots of scrunched data

    #Get template from template_id and verify MD5SUM

    #Generate TOA with pat

    #Make plots associated with the TOA generation

    #Insert TOA into DB

    #Close DB connection
    print "Closing DB connection..."
    DBconn.close()

    #End pipeline
    print "###################################################"
    print "Finished EPTA Timing Pipeline Version %.2f"%VERSION
    print "End time: %s"%epta.Give_UTC_now()
    print "###################################################"

if __name__ == "__main__":
    main()


