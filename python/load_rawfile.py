#!/usr/bin/python
####################
# load_rawfile.py #
VERSION = 0.1
####################

#Imported modules
import sys
from os import system, popen
from MySQLdb import *
import os.path
import datetime
import glob

# import pipeline utilities
import epta_pipeline_utils as epu

#Database parameters
DB_HOST = "localhost"
DB_NAME = "epta"
DB_USER = "epta"
DB_PASS = "mysqlaccess"

#Python version to use
PYTHON = "/usr/bin/python"

#Storage directories
interfile_path="/home/epta/database/data/interfiles"

#Debugging flags
VERBOSE = 1 #Print extra output
TEST = 0 #Prints commands and other actions without running them

def Help():
    print "\nLoad raw files to database"
    print "Version: %.2f"%(VERSION)+"\n"
    print ("'%s' accepts only raw files. Wildcard is allowed.\n")% sys.argv[0]
    sys.exit(0)

def Parse_psrfits_header(file):
    param_names = []
    param_values = []
    system("psredit %s > psredit.tmp"%file)
    lines = open("psredit.tmp","r").readlines()
    for line in lines[2:-1]:
        line_split = line.split()
        param_name = line_split[0].strip()
        if "*" in param_name:               
            p = popen("psredit -c %s %s"%(param_name.replace("*","[0]"),file),"r")
            param_val = p.readline().split("=")[-1].strip()
        else:
            param_val = line_split[-1].strip()
        param_names.append(param_name)
        param_values.append(param_val)
    return param_names, param_values

#Maps psrfits header values to the DB column names
def Map_param_2_DB(param_name,param_val):
    if "*:" in param_name:
        param_name = param_name.replace("*:","_")
    if ":" in param_name:
        param_name = param_name.replace(":","_")
    if "*" in param_name:
        param_name = param_name.replace("*","")
    if "nan" in param_val:
        param_val = "NULL"
    if "_mjd" in param_name:
        param_name = ["int_imjd", "int_fmjd"]
        param_val = [str(int(float(param_val))),str(float(param_val)-int(float(param_val)))]
    else:
        param_name = [param_name]
        param_val = [Remove_units(param_val)]
    return zip(param_name, param_val)

#Parses out the parfile info from the psrfits file
def Parse_psrfits_parfile(file):
    parfile_names = []
    parfile_values = []
    system("vap -E %s > parfile.tmp"%file)
    lines = open("parfile.tmp","r").readlines()
    for line in lines[1:]:
        line_split = line.split()
        parfile_names.append(line_split[0].strip())
        parfile_values.append(line_split[1].strip())
    return zip(parfile_names,parfile_values)

#Remove units from a parameter... may need to be extended
def Remove_units(param):
    param = param.strip('deg')
    return param

def DB_injest_psrfits(file,data_type,proc_id,DBcursor,DBconn,verbose=0):
    #Determine path (will retrieve absolute path)
    file_path, file_name = Verify_file_path(file, verbose=VERBOSE)

    #Parse the psredit output
    print "Importing header information for %s"%file
    print "This file has type: %s"%data_type
    param_names, param_values = Parse_psrfits_header(file)
    
    #Create new DB row with proc_id, file_name, and file_path
    QUERY = "INSERT INTO psrfits SET proc_id = '%s'"%(proc_id)
    DBcursor.execute(QUERY)
    #Get psrfits_id
    QUERY = "SELECT LAST_INSERT_ID()"
    DBcursor.execute(QUERY)
    psrfits_id = DBcursor.fetchall()[0][0]
    QUERY = "UPDATE psrfits SET file_name = '%s' WHERE psrfits_id = '%s'"%(file_name,psrfits_id)
    DBcursor.execute(QUERY)
    QUERY = "UPDATE psrfits SET file_path = '%s' WHERE psrfits_id = '%s'"%(file_path,psrfits_id)
    DBcursor.execute(QUERY)

    #Insert values into psrfits table
    for param in zip(param_names, param_values)[1:]:
        for val in Map_param_2_DB(param[0],param[1]):
            if val[1] == "NULL":
                QUERY = "UPDATE psrfits SET %s = NULL WHERE psrfits_id = '%s'"%(val[0],psrfits_id)
            else:
                QUERY = "UPDATE psrfits SET %s = '%s' WHERE psrfits_id = '%s'"%(val[0],val[1],psrfits_id)
                try:
                    DBcursor.execute(QUERY)
                except (OperationalError, ProgrammingError):
                    if verbose:
                        print "Warning: No corresponding parameter in DB table 'psrfits'!"

    QUERY = "SELECT psrfits_id, file_name, nbin, nchan, npol, nsubint, site FROM psrfits WHERE psrfits_id = '%s'"%psrfits_id
    DBcursor.execute(QUERY)
    DBOUT = DBcursor.fetchall()[0]
    if verbose:
        print "psrfits_id\tfile_name\tnbin\tnchan\tnpol\tnsubint\tsite"
        print "\t".join("%s" % val for val in DBOUT[0:7])

    # Update psrfits table with data type
    QUERY = "update psrfits set datatype='%s' where psrfits_id='%s'"%(data_type,psrfits_id)
    DBcursor.execute(QUERY)

    #Insert values into parfile table
    print "Importing parfile information for %s"%file
    QUERY = "INSERT INTO parfile SET proc_id = '%s'"%(proc_id)
    DBcursor.execute(QUERY)               
    #Get par_id
    QUERY = "SELECT LAST_INSERT_ID()"
    DBcursor.execute(QUERY)
    par_id = DBcursor.fetchall()[0][0]
    #Insert psrfits_id into parfile table
    QUERY = "UPDATE parfile SET psrfits_id = '%s' WHERE par_id = '%s'"%(psrfits_id,par_id)
    DBcursor.execute(QUERY)    
    for param in Parse_psrfits_parfile(file):
        QUERY = "UPDATE parfile SET %s = '%s' WHERE par_id = '%s'"%(param[0],param[1],par_id)
        try:
            DBcursor.execute(QUERY)                              
        except (OperationalError, ProgrammingError):
            if verbose:
                print "Warning: No corresponding parameter in DB table 'parfile'!"

    QUERY = "SELECT par_id, PSRJ, F0, DM FROM parfile WHERE par_id = '%s'"%par_id
    DBcursor.execute(QUERY)
    DBOUT = DBcursor.fetchall()[0]
    par_id = DBOUT[0]
    if verbose:
        print "par_id\tPSRJ\tF0\tDM"
        print "\t".join("%s" % val for val in DBOUT[0:4])

    return psrfits_id, par_id

def DB_pam(psrfits_id,proc_id,pipeline_id,DBcursor,DBconn,data_type='intermediate'):

    #Stores the IDs of the intermediate files that are created
    intermediate_ids = []

    # Generate and execute QUERY
    QUERY = "select file_path,file_name from psrfits where psrfits_id=%s"%psrfits_id
    DBcursor.execute(QUERY)

    # Fetch QUERY
    result = DBcursor.fetchall()[0]
    file = os.path.join(result[0],result[1])
    filepath, filename = os.path.split(file)
    extn = filename[ filename.rindex( '.' ):len( filename ) ]
    interfile_base = os.path.join(interfile_path,filename.strip( extn )) 

    # Frequency scrunched
    COMMAND = "pam -u %s -F -e Ft %s"%(interfile_path,file)
    epu.Run_shell_command(COMMAND, verbose=VERBOSE, test=TEST)
    # Injest
    interfile_id, par_id = DB_injest_psrfits("%s.Ft"%interfile_base,data_type,proc_id,DBcursor,DBconn,verbose=0)
    # Store process
    QUERY = "insert into process (psrfits_id,product_id,pipeline_id) values (%s,%s,%s)"%(psrfits_id,interfile_id,pipeline_id)
    print QUERY
    DBcursor.execute(QUERY)
    intermediate_ids.append(interfile_id)

    # Time scrunched
    COMMAND = "pam -u %s -T -e fT %s"%(interfile_path,file)
    epu.Run_shell_command(COMMAND, verbose=VERBOSE, test=TEST)
    # Injest
    interfile_id, par_id = DB_injest_psrfits("%s.fT"%interfile_base,data_type,proc_id,DBcursor,DBconn,verbose=0)
    # Store process
    QUERY = "insert into process (psrfits_id,product_id,pipeline_id) values (%s,%s,%s)"%(psrfits_id,interfile_id,pipeline_id)
    DBcursor.execute(QUERY)
    intermediate_ids.append(interfile_id)

    # 8x8 scrunched
    COMMAND = "pam -u %s --setnsub 8 --setnchn 8 -e 88 %s"%(interfile_path,file)
    epu.Run_shell_command(COMMAND, verbose=VERBOSE, test=TEST)
    # Injest
    interfile_id, par_id = DB_injest_psrfits("%s.88"%interfile_base,data_type,proc_id,DBcursor,DBconn,verbose=0)
    # Store process
    QUERY = "insert into process (psrfits_id,product_id,pipeline_id) values (%s,%s,%s)"%(psrfits_id,interfile_id,pipeline_id)
    DBcursor.execute(QUERY)
    intermediate_ids.append(interfile_id)

    # Frequency and time scrunched
    COMMAND = "pam -u %s -FT -e FT %s"%(interfile_path,file)
    epu.Run_shell_command(COMMAND, verbose=VERBOSE, test=TEST)
    # Injest
    interfile_id, par_id = DB_injest_psrfits("%s.FT"%interfile_base,data_type,proc_id,DBcursor,DBconn,verbose=0)
    # Store process
    QUERY = "insert into process (psrfits_id,product_id,pipeline_id) values (%s,%s,%s)"%(psrfits_id,interfile_id,pipeline_id)
    DBcursor.execute(QUERY)
    #Grab the psrfits_id of the scrunched .FT file
    intermediate_ids.append(interfile_id)

    # Fully scrunched
    COMMAND = "pam -u %s -FTp -e FTp %s"%(interfile_path,file)
    epu.Run_shell_command(COMMAND, verbose=VERBOSE, test=TEST)
    # Injest
    interfile_id, par_id = DB_injest_psrfits("%s.FTp"%interfile_base,data_type,proc_id,DBcursor,DBconn,verbose=0)
    # Store process
    QUERY = "insert into process (psrfits_id,product_id,pipeline_id) values (%s,%s,%s)"%(psrfits_id,interfile_id,pipeline_id)
    DBcursor.execute(QUERY)
    intermediate_ids.append(interfile_id)
    scrunched_id = interfile_id

    return scrunched_id, intermediate_ids

def DB_pav(psrfits_ids,DBcursor,DBconn):
    for psrfits_id in psrfits_ids:
        query = "select file_path,file_name from psrfits where datatype='intermediate' and psrfits_id='%i'"%psrfits_id
        DBcursor.execute(query)
        result = DBcursor.fetchall()

        if not len(result):
            print "No values found"
            pass
        else:
            result = result[0]

            file = os.path.join(result[0],result[1])
            filepath, filename = os.path.split(file)

            file_ext = filename.split(".")[-1]
            if file_ext == "fT":
                command = "pav -dG %s -g %s.png/png"%(file,file)
                epu.Run_shell_command(command, verbose=VERBOSE, test=TEST)
            if file_ext == "Ft":
                command = "pav -Y %s -g %s.png/png"%(file,file)
                epu.Run_shell_command(command, verbose=VERBOSE, test=TEST)
            if file_ext == "FT":
                command = "pav -S %s -g %s.png/png"%(file,file)
                epu.Run_shell_command(command, verbose=VERBOSE, test=TEST)
            if file_ext == "FTp":
                command = "pav -DFTp %s -g%s.png/png"%(file,file)
                epu.Run_shell_command(command, verbose=VERBOSE, test=TEST)

def Run_loader(file,proc_id,std_prof_id):

    #Make DB connection
    DBcursor, DBconn = epu.DBconnect(DB_HOST,DB_NAME,DB_USER,DB_PASS)

    #Fill pipeline table
    pipeline_id = epu.Fill_pipeline_table(DBcursor,DBconn)

    #Run DB_injest_psrfits
    print "\n*** Starting %s at %s"%(DB_injest_psrfits.__name__,epu.Give_UTC_now())
    psrfits_id, par_id = DB_injest_psrfits(file,'raw',proc_id,DBcursor,DBconn,verbose=VERBOSE)
    print "*** Finished %s at %s\n"%(DB_injest_psrfits.__name__,epu.Give_UTC_now())
    print "*** %s returned psrfits_id: %s and par_id: %s"%(DB_injest_psrfits.__name__,psrfits_id,par_id)

    #Run DB_pam
    print "\n*** Starting %s at %s"%(DB_pam.__name__,epu.Give_UTC_now())
    scrunched_id, intermediate_ids = DB_pam(psrfits_id,proc_id,pipeline_id,DBcursor,DBconn)
    print "*** Finished %s at %s\n"%(DB_pam.__name__,epu.Give_UTC_now())
    print "*** %s returned scrunched_id: %s and intermediate_ids: "%(DB_injest_psrfits.__name__,scrunched_id)+",".join("%s" % val for val in intermediate_ids)

    #Run DB_pav
    print "\n*** Starting %s at %s"%(DB_pav.__name__,epu.Give_UTC_now())
    DB_pav(intermediate_ids,DBcursor,DBconn)
    print "*** Finished %s at %s\n"%(DB_pav.__name__,epu.Give_UTC_now())

    #Close DB connection
    DBconn.close()

def main():

    if len(sys.argv) > 1:

        flist0=[];
        flist1=[];
        # Check if raw files exist. If wildcard was entered, check validity of all files. 
        # If there is "*" or "?" character that was not interpreted by shell,
        # use glob interpret it.
        for i in range(len(sys.argv)):
            if ("*" in sys.argv[i]) or ("?" in sys.argv[i]):
                flist0 = glob.glob(sys.argv[i])
            else:
                flist1.append(sys.argv[i])
        flist = flist0 + flist1;

        for file in flist:
            epn.Verify_file_path(file)
        
        # Make DB connection
        DBcursor, DBconn = DBconnect(DB_HOST,DB_NAME,DB_USER,DB_PASS)

        # Load files and populate the tables
        Run_loader();
        
        #Close DB connection
        DBconn.close()

        print "########################################################"
        print "Successfully uploaded raw files to the EPTA DB"
        print "End time: %s"%epu.Give_UTC_now()
        print "########################################################"
    else:
        print "\nNo files to process.  Exiting..."
        Help()
        
main()

