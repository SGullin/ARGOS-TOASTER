#!/usr/bin/python

#Imported modules
import sys
import epta_pipeline_utils as epta
import epta_timing_pipeline as epta_core

import load_rawfile

#Database parameters
DB_HOST = "localhost"
DB_NAME = "epta"
DB_USER = "epta"
DB_PASS = "psr1937"

def Parse_command_line():
    parser = epta.DefaultArguments(
        prog='run_pipeline',
        description='')
    #Rawfile name
    parser.add_argument('--rawfile',
                        type=str,
                        default=None,
                        help="Raw data file to upload and use for running the full pipeline.")
    #Number of chans for scrunched archive
    parser.add_argument('--nchan',
                        type=int,
                        default=1,
                        help="Number of chans for scrunched archive. (Default: 1)")
    #Number of sub-intervals for scrunched archive
    parser.add_argument('--nsub',
                        type=int,
                        default=1,
                        help="Number of sub-intervals for scrunched archive. (Default: 1)")
    #Manually specified DM
    parser.add_argument('--DM',
                        type=float,
                        default=None,
                        help="Manually specified DM (This argument is not used at the moment!)")
                            
    args=parser.parse_args()
    return args

def Help():
    print "\nWrapper to The EPTA Timing Pipeline"
    sys.exit(0)

#Make DB connection
DBcursor, DBconn = epta.DBconnect(DB_HOST,DB_NAME,DB_USER,DB_PASS)

def main():

    #Exit if there are no or insufficient arguments
    if len(sys.argv) < 2:
        Help()

    args = Parse_command_line()
    rawfile = args.rawfile
    nchan = args.nchan
    nsub = args.nsub
    DM = args.DM
    
    #Load rawfile into archive
    rawfile_id = load_rawfile.load_rawfile(rawfile)
    print "Using rawfile_id = %d"%rawfile_id

    #Get pulsar_id and obssystem_id
    query = "select pulsar_id, obssystem_id from rawfiles where rawfile_id = %d"%(rawfile_id)
    DBcursor.execute(query)
    pulsar_id, obssystem_id = DBcursor.fetchall()[0]
    pulsar_id = int(pulsar_id)
    obssystem_id = int(obssystem_id)
    print "Using pulsar_id = %d"%pulsar_id
    print "Using obssystem_id = %d"%obssystem_id
    
    #Determine master template_id
    query = "select template_id from master_templates where pulsar_id = %d and obssystem_id = %d"%(pulsar_id,obssystem_id)
    DBcursor.execute(query)
    template_id = DBcursor.fetchall()[0][0]
    template_id = int(template_id)
    print "Using master template_id = %d"%template_id

    #Determine master parfile_id
    query = "select master_parfile_id from pulsars where pulsar_id = %d"%(pulsar_id)
    DBcursor.execute(query)
    master_parfile_id = DBcursor.fetchall()[0][0]    
    master_parfile_id = int(master_parfile_id)
    print "Using master parfile_id = %d"%master_parfile_id

    #Run pipeline script
    epta_core.pipeline_core(rawfile_id,master_parfile_id,template_id,nchan,nsub,DM)

if __name__ == "__main__":
    main()
