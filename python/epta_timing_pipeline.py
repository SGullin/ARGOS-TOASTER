#!/usr/bin/python
####################
# epta_pipeline.py #
PIPE_NAME = "epta_timing_pipeline"
VERSION = 0.2
####################

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
DB_PASS = "mysqlaccess"

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
        
    print "Please use 'epta_pipeline.py -h' for a full list of command line options. \n"

    print "\n"
    exit(0)

def Parse_command_line():
    parser = argparse.ArgumentParser(
        prog='epta_pipeline',
        description='')
    #Standard profile
    parser.add_argument('--std_prof_id',
                        nargs=1,
                        type=int,
                        default=None,
                        help="psrfits_id of standard profile to use for running the full pipeline.")
    #Do a full pipeline run
    parser.add_argument('--full_pipeline',
                        nargs=1,
                        type=str,
                        default=None,
                        help="Run the full pipeline.")
    args=parser.parse_args()
    return args


def main():

    if len(argv) < 2:
        Help()

    args = Parse_command_line()

    if args.full_pipeline and args.std_prof_id:
        print "###################################################"
        print "Starting EPTA Timing Pipeline Version %.2f"%VERSION
        proc_id = Make_Proc_ID()
        print "Proc ID (UTC start datetime): %s"%proc_id
        print "Start time: %s"%Give_UTC_now()
        print "###################################################"

        file = args.full_pipeline[0]
        std_prof_id = args.std_prof_id[0]
        file_path, file_name = Verify_file_path(file, verbose=VERBOSE)
        print "Running on %s"%os.path.join(file_path, file_name)
        Run_EPTA_pipeline(file,proc_id,std_prof_id)

        print "###################################################"
        print "Finished EPTA Timing Pipeline Version %.2f"%VERSION
        print "End time: %s"%Give_UTC_now()
        print "###################################################"
    elif args.std_prof:
        std_prof = args.std_prof[0]
        print "########################################################"
        print "Uploading a standard profile %s to the EPTA DB"%std_prof
        proc_id = Make_Proc_ID()
        print "Proc ID (UTC start datetime): %s"%proc_id
        print "Start time: %s"%Give_UTC_now()
        print "########################################################"
        
        #Make DB connection
        DBcursor, DBconn = DBconnect(DB_HOST,DB_NAME,DB_USER,DB_PASS)

        #Fill pipeline table
        pipeline_id = Fill_pipeline_table(DBcursor,DBconn)

        psrfits_id, par_id = DB_injest_psrfits(std_prof,'std_prof',proc_id,DBcursor,DBconn,verbose=VERBOSE)

        #Close DB connection
        DBconn.close()

        print "########################################################"
        print "Successfully uploaded standard profile %s to the EPTA DB"%std_prof
        print "This standard profile has psrfits_id = %d"%psrfits_id
        print "End time: %s"%Give_UTC_now()
        print "########################################################"
    else:
        print "\nYou haven't specified a valid set of command line options.  Exiting..."
        Help()
        
main()
