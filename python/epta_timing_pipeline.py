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
import manipulators
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

    print "To run a test on J1713+0747 use rawfile_id=80 parfile_id=15 and template_id=3"
    print "i.e. run './epta_timing_pipeline.py --rawfile_id 80 --parfile_id 15 --template_id 3 --debug'"
        
    print "Please use 'epta_timing_pipeline.py -h' for a full list of command line options. \n"

    print "\n"
    exit(0)

def Parse_command_line():
    parser = epta.DefaultArguments(
        prog='epta_pipeline',
        description='')
    #Raw data
    parser.add_argument('--rawfile_id',
                        type=int,
                        default=None,
                        help="ID of raw data file to use for running the full pipeline.")
    #Ephemeris
    parser.add_argument('--parfile_id',
                        type=int,
                        default=None,
                        help="ID of ephemeris to use for running the full pipeline.")
    #Template profile
    parser.add_argument('--template_id',
                        type=int,
                        default=None,
                        help="ID of template profile to use for running the full pipeline.")
    #Number of chans for scrunched archive
    parser.add_argument('--nchan',
                        type=int,
                        default=1,
                        help="Number of chans for scrunched archive")
    #Number of sub-intervals for scrunched archive
    parser.add_argument('--nsub',
                        type=int,
                        default=1,
                        help="Number of sub-intervals for scrunched archive")
    #Manually specified DM
    parser.add_argument('--DM',
                        type=int,
                        default=None,
                        help="Manually specified DM")
    
    args=parser.parse_args()
    return args

def pipeline_core(rawfile_id,parfile_id,template_id,nchan,nsub,DM):
    #Start pipeline
    print "###################################################"
    print "Starting EPTA Timing Pipeline Version %.2f"%VERSION
    proc_id = epta.Make_Proc_ID()
    print "Proc ID (UTC start datetime): %s"%proc_id
    print "Start time: %s"%epta.Give_UTC_now()
    print "###################################################"

    #Temporary? Source to get right environment
    epta.execute("source /raid1/home/epta/login/bash.bashrc")
    epta.execute("source /raid1/home/epta/login/bash.bash_profile")

    #Make DB connection
    DBcursor, DBconn = epta.DBconnect(DB_HOST,DB_NAME,DB_USER,DB_PASS)

    #Fill pipeline table
    fake_command_line = "epta_timing_pipeline.py --rawfile_id %d --parfile_id %d --template_id %d" % (rawfile_id,parfile_id,template_id)
    if nchan != 1:
        fake_command_line += " --nchan %d" % nchan
    if nsub != 1:
        fake_commnd_line += " --nsub %d" % nsub
    if DM is not None:
        fake_command_line += " --DM %d" % DM

    process_id = epta.Fill_process_table(DBcursor,VERSION,rawfile_id,parfile_id,template_id,fake_command_line,nchan,nsub)
    
    #Get raw data from rawfile_id and verify MD5SUM
    raw_file, raw_file_name = epta.get_file_and_id('rawfile',rawfile_id,DBcursor)
        
    #Get ephemeris from parfile_id and verify MD5SUM
    parfile, parfile_name = epta.get_file_and_id('parfile',parfile_id,DBcursor)

    #Scrunch data in time/freq and optionally re-install ephemeris and change DM
    #Use Patrick's manipulator
    scrunch_file = raw_file_name.split(".")[0]+".scrunch"
    manipulators.pamit.manipulate([raw_file], scrunch_file, nsub=nsub, nchan=nchan, nbin=None)

    #Make diagnostic plots of scrunched data
    epta.execute("pav -g '%s.ps/CPS' -DFTp %s"%(scrunch_file,scrunch_file))

    #Get template from template_id and verify MD5SUM
    template, template_name = epta.get_file_and_id('template',template_id,DBcursor)

    #Generate TOA with pat
    stdout, stderr = epta.execute("pat -s %s %s"%(template,scrunch_file))
    
    #Make plots associated with the TOA generation

    #Insert TOA into DB
    for toa in stdout.split("\n"):
        toa = toa.strip()
        if toa:
            print toa
            epta.DB_load_TOA(toa,DBcursor,template_id,rawfile_id)

    #Close DB connection
    print "Closing DB connection..."
    DBconn.close()

    #End pipeline
    print "###################################################"
    print "Finished EPTA Timing Pipeline Version %.2f"%VERSION
    print "End time: %s"%epta.Give_UTC_now()
    print "###################################################"    

def main():

    #Exit if there are no or insufficient arguments
    if len(argv) < 2:
        Help()

    args = Parse_command_line()

    if not (args.rawfile_id and args.parfile_id and args.template_id):
        print "\nYou haven't specified a valid set of command line options.  Exiting..."
        Help()

    #Grab command line options
    rawfile_id = args.rawfile_id
    parfile_id = args.parfile_id
    template_id = args.template_id
    nsub = args.nsub
    nchan = args.nchan
    DM = args.DM

    #Run pipeline core
    pipeline_core(rawfile_id,parfile_id,template_id)

if __name__ == "__main__":
    main()
