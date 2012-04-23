import sys
import epta_pipeline_utils as epta

#Database parameters
DB_HOST = "localhost"
DB_NAME = "epta"
DB_USER = "epta"
DB_PASS = "psr1937"

#Make DB connection
DBcursor, DBconn = epta.DBconnect(DB_HOST,DB_NAME,DB_USER,DB_PASS)

for rawfile in sys.argv[1:]:

    #Load rawfile into archive
    stdout, stderr = epta.execute("./load_rawfile.py %s"%rawfile)
    rawfile_id = stdout.split(" ")[-1].strip()
    rawfile_id = int(rawfile_id)
    print rawfile_id

    #Get pulsar_id and obssystem_id
    query = "select pulsar_id, obssystem_id from rawfiles where rawfile_id = %d"%(rawfile_id)
    DBcursor.execute(query)
    pulsar_id, obssystem_id = DBcursor.fetchall()[0]
    pulsar_id = int(pulsar_id)
    obssystem_id = int(obssystem_id)
    print pulsar_id, obssystem_id
    
    #Determine master template_id
    query = "select template_id from master_templates where pulsar_id = %d and obssystem_id = %d"%(pulsar_id,obssystem_id)
    DBcursor.execute(query)
    template_id = DBcursor.fetchall()[0][0]
    print template_id
    template_id = int(template_id)
    print template_id

    #Determine master parfile_id
    query = "select master_parfile_id from pulsars where pulsar_id = %d"%(pulsar_id)
    DBcursor.execute(query)
    master_parfile_id = DBcursor.fetchall()[0][0]    
    master_parfile_id = int(master_parfile_id)
    print master_parfile_id

    #Run pipeline script
    epta.execute("./epta_timing_pipeline.py --rawfile_id %d --parfile_id %d --template_id %d --debug"%(rawfile_id,master_parfile_id,template_id),stdout=sys.stdout)
