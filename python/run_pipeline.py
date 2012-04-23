from sys import argv
import epta_pipeline_utils as epta
from os import exit

for rawfile in argv:

    #Load rawfile into archive
    stdout, stderr = epta.execute("./load_rawfile.py %s"%rawfile)
    rawfile_id = stdout.split(" ")[-1].strip()
    print rawfile_id
    exit(0)

    #Get pulsar_id and obssystem_id
    query = "select pulsar_id, obssystem_id from rawfiles where rawfile_id = %d"%(rawfile_id)
    DBcursor.execute(query)
    pulsar_id, obssystem_id = DBcursor.fetchall()
    
    #Determine master template_id
    query = "select master_parfile_id from pulsars where pulsar_id = %d"%(pulsar_id)

    #Determine master parfile_id
    query = "select template_id from master_templates where pulsar_id = %d and obssystem_id = %d"%(pulsar_id,obssystem_id)
    DBcursor.execute(query)
    master_parfile_id = DBcursor.fetchall()

    #Run pipeline script
    epta.execute("./epta_timing_pipeline.py --rawfile_id %d --parfile_id %d --template_id %d --debug")%(rawfile_id,parfile_id,template_id)
