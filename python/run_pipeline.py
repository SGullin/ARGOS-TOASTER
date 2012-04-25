#!/usr/bin/python

#Imported modules
import sys
import epta_pipeline_utils as epta
import epta_timing_pipeline as epta_core

import load_rawfile
import database
import errors

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
                        help="Manually specified DM. (This argument is not used at the moment!)")

    args=parser.parse_args()
    return args

def Help():
    print "\nWrapper to The EPTA Timing Pipeline"
    sys.exit(0)

#Make DB connection
db = database.Database()

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

    #Get ID numbers for master parfile and master template
    query = "SELECT mtmp.template_id, " \
                "psr.master_parfile_id " \
            "FROM rawfiles AS r " \
            "LEFT JOIN master_templates AS mtmp " \
                "ON mtmp.obssystem_id=r.obssystem_id " \
                    "AND mtmp.pulsar_id=r.pulsar_id " \
            "LEFT JOIN pulsars AS psr " \
                "ON psr.pulsar_id=r.pulsar_id " \
            "WHERE rawfile_id=%d" % (rawfile_id)
    db.execute(query)
    template_id, master_parfile_id = db.fetchone()
    if template_id is None:
        raise errors.DatabaseError("There is no approriate master template " \
                    "for this input file. Please add one using " \
                    "'load_template.py' with the '--master' flag.")
    else:
        print "Using template_id: %d" % template_id
    if master_parfile_id is None:
        raise errors.DatabaseError("There is no approriate master parfile " \
                    "for this input file. Please add one using " \
                    "'load_parfile.py' with the '--master' flag.")
    else:
        print "Using master_parfile_id: %d" % master_parfile_id
    
    #Run pipeline script
    print rawfile_id, master_parfile_id,template_id,nchan,nsub,DM
    epta_core.pipeline_core(rawfile_id,master_parfile_id,template_id,nchan,nsub,DM)

if __name__ == "__main__":
    main()
