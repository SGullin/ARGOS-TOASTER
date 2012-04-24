#!/usr/bin/python

#Imported modules
import sys
import epta_pipeline_utils as epta
import epta_timing_pipeline as epta_core

#Database parameters
DB_HOST = "localhost"
DB_NAME = "epta"
DB_USER = "epta"
DB_PASS = "psr1937"

def Parse_command_line():
    parser = epta.DefaultArguments(
        prog='run_pipeline',
        description='')
    parser.add_argument('--rawfile',
                        nargs=1,
                        type=str,
                        default=None,
                        help="Raw data file to upload and use for running the full pipeline.")
    
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
    rawfile = args.rawfile[0]
    
    #Load rawfile into archive
    stdout, stderr = epta.execute("./load_rawfile.py %s"%rawfile)
    rawfile_id = stdout.split(" ")[-1].strip()
    rawfile_id = int(rawfile_id)
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
    epta_core.pipeline_core(rawfile_id,master_parfile_id,template_id)

if __name__ == "__main__":
    main()
