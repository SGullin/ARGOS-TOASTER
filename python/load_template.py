#!/usr/bin/python
####################
# load_template.py #
SCRIPT_NAME = "load_template"
VERSION = 0.1
####################

#Imported modules
from sys import argv, exit
from os import system, popen
from MySQLdb import *
import os.path
import datetime
import argparse

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

#Storage directories
interfile_path="/home/epta/database/data/interfiles"

#Debugging flags
VERBOSE = 1 #Print extra output
TEST = 0 #Prints commands and other actions without running them

###############################################################################
# DO NOT EDIT BELOW HERE
###############################################################################

#Functions
def Help():
    #Print basic description and usage
    print "\nPython script to upload templates"
    print "Version: %.2f"%(VERSION)+"\n"
        
    print "Please use '%s"%(SCRIPT_NAME)+".py -h' for a full list of command line options. \n"

    print "\n"
    exit(0)

def Parse_command_line():
    parser = argparse.ArgumentParser(
        prog=SCRIPT_NAME+'.py',
        description='')
    parser.add_argument('--template',
                        nargs=1,
                        type=str,
                        default=None,
                        help="File name of the template to upload.")
    parser.add_argument('--pulsar',
                        nargs=1,
                        type=str,
                        default=None,
                        help="Pulsar name (if not specified in template header).")
    parser.add_argument('--system',
                        nargs=1,
                        type=str,
                        default=None,
                        help="Observing system (if not specified in template header).")
    parser.add_argument('--comments',
                        nargs=1,
                        type=str,
                        default=None,
                        help="Provide comments describing the template.")
    args=parser.parse_args()
    return args

def DBconnect(Host,DBname,Username,Password):
    #To make a connection to the database
    try:
        connection = connect(host=Host,db=DBname,user=Username,passwd=Password)
        cursor = connection.cursor()
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

def Parse_psrfits_header(file):
    #Parses out the psrfits header info using psredit
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

def Map_param_2_DB(param_name,param_val):
    #Maps psrfits header values to the DB column names
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

def Parse_psrfits_parfile(file):
    #Parses out the parfile info from the psrfits file
    parfile_names = []
    parfile_values = []
    system("vap -E %s > parfile.tmp"%file)
    lines = open("parfile.tmp","r").readlines()
    for line in lines[1:]:
        line_split = line.split()
        parfile_names.append(line_split[0].strip())
        parfile_values.append(line_split[1].strip())
    return zip(parfile_names,parfile_values)

def Remove_units(param):
    #Remove units from a parameter... may need to be extended
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

    interfile_base = os.path.join(interfile_path,filename.strip(".ar")) 

    # Frequency scrunched
    COMMAND = "pam -u %s -F -e Ft %s"%(interfile_path,file)
    Run_shell_command(COMMAND, verbose=VERBOSE, test=TEST)
    # Injest
    interfile_id, par_id = DB_injest_psrfits("%s.Ft"%interfile_base,data_type,proc_id,DBcursor,DBconn,verbose=0)
    # Store process
    QUERY = "insert into process (psrfits_id,product_id,pipeline_id) values (%s,%s,%s)"%(psrfits_id,interfile_id,pipeline_id)
    print QUERY
    DBcursor.execute(QUERY)
    intermediate_ids.append(interfile_id)

    # Time scrunched
    COMMAND = "pam -u %s -T -e fT %s"%(interfile_path,file)
    Run_shell_command(COMMAND, verbose=VERBOSE, test=TEST)
    # Injest
    interfile_id, par_id = DB_injest_psrfits("%s.fT"%interfile_base,data_type,proc_id,DBcursor,DBconn,verbose=0)
    # Store process
    QUERY = "insert into process (psrfits_id,product_id,pipeline_id) values (%s,%s,%s)"%(psrfits_id,interfile_id,pipeline_id)
    DBcursor.execute(QUERY)
    intermediate_ids.append(interfile_id)

    # 8x8 scrunched
    COMMAND = "pam -u %s --setnsub 8 --setnchn 8 -e 88 %s"%(interfile_path,file)
    Run_shell_command(COMMAND, verbose=VERBOSE, test=TEST)
    # Injest
    interfile_id, par_id = DB_injest_psrfits("%s.88"%interfile_base,data_type,proc_id,DBcursor,DBconn,verbose=0)
    # Store process
    QUERY = "insert into process (psrfits_id,product_id,pipeline_id) values (%s,%s,%s)"%(psrfits_id,interfile_id,pipeline_id)
    DBcursor.execute(QUERY)
    intermediate_ids.append(interfile_id)

    # Frequency and time scrunched
    COMMAND = "pam -u %s -FT -e FT %s"%(interfile_path,file)
    Run_shell_command(COMMAND, verbose=VERBOSE, test=TEST)
    # Injest
    interfile_id, par_id = DB_injest_psrfits("%s.FT"%interfile_base,data_type,proc_id,DBcursor,DBconn,verbose=0)
    # Store process
    QUERY = "insert into process (psrfits_id,product_id,pipeline_id) values (%s,%s,%s)"%(psrfits_id,interfile_id,pipeline_id)
    DBcursor.execute(QUERY)
    #Grab the psrfits_id of the scrunched .FT file
    scrunched_id = interfile_id
    intermediate_ids.append(interfile_id)

    # Fully scrunched
    COMMAND = "pam -u %s -FTp -e FTp %s"%(interfile_path,file)
    Run_shell_command(COMMAND, verbose=VERBOSE, test=TEST)
    # Injest
    interfile_id, par_id = DB_injest_psrfits("%s.FTp"%interfile_base,data_type,proc_id,DBcursor,DBconn,verbose=0)
    # Store process
    QUERY = "insert into process (psrfits_id,product_id,pipeline_id) values (%s,%s,%s)"%(psrfits_id,interfile_id,pipeline_id)
    DBcursor.execute(QUERY)
    intermediate_ids.append(interfile_id)

    return scrunched_id, intermediate_ids


def DB_pat(std_id,scrunched_id,DBcursor,DBconn):

    #Gets paths and file name of the STANDARD file  
    QUERY ="select file_path, file_name from psrfits where psrfits_id=%s"%(std_id)
    DBcursor.execute(QUERY)
    # Fetch the result
    result = DBcursor.fetchall()[0]
    std_file = os.path.join(result[0],result[1])
    
    #Gets paths and file name of the INTER file (SCRUNCHED FILE)  
    QUERY ="select file_path, file_name, name from psrfits where psrfits_id=%s"%(scrunched_id)
    DBcursor.execute(QUERY)
    # Fetch the result
    result = DBcursor.fetchall()[0]
    scrunched_file = os.path.join(result[0],result[1])
    psr_name = result[2]
    
    # The pat command:
    patc = "pat -s %s %s"%(std_file,scrunched_file)
    
    # Runs the pat command and gets values
    toa = popen(patc,"r").readlines()[0]
    freq = toa.split()[1]
    imjd = toa.split()[2].split(".")[0]
    fmjd = "0." + toa.split()[2].split(".")[1]
    errmjd = toa.split()[4]
    obs = toa.split()[5]

    # Writes values to the toa table
    QUERY = "insert into toa (std_id,psrfits_id,psr_name,imjd,fmjd,freq,mjd_err,obs,pat_command) values ('%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(std_id,scrunched_id,psr_name,imjd,fmjd,freq,errmjd,obs,patc)
    DBcursor.execute(QUERY)

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
                Run_shell_command(command, verbose=VERBOSE, test=TEST)
            if file_ext == "Ft":
                command = "pav -Y %s -g %s.png/png"%(file,file)
                Run_shell_command(command, verbose=VERBOSE, test=TEST)
            if file_ext == "FT":
                command = "pav -S %s -g %s.png/png"%(file,file)
                Run_shell_command(command, verbose=VERBOSE, test=TEST)
            if file_ext == "FTp":
                command = "pav -DFTp %s -g%s.png/png"%(file,file)
                Run_shell_command(command, verbose=VERBOSE, test=TEST)

def Run_EPTA_pipeline(file,proc_id,std_prof_id):

    #Make DB connection
    DBcursor, DBconn = DBconnect(DB_HOST,DB_NAME,DB_USER,DB_PASS)

    #Fill pipeline table
    pipeline_id = Fill_pipeline_table(DBcursor,DBconn)

    #Run DB_injest_psrfits
    print "\n*** Starting %s at %s"%(DB_injest_psrfits.__name__,Give_UTC_now())
    psrfits_id, par_id = DB_injest_psrfits(file,'raw',proc_id,DBcursor,DBconn,verbose=VERBOSE)
    print "*** Finished %s at %s\n"%(DB_injest_psrfits.__name__,Give_UTC_now())
    print "*** %s returned psrfits_id: %s and par_id: %s"%(DB_injest_psrfits.__name__,psrfits_id,par_id)

    #Run Zap

    #Update parfile?

    #Run DB_pam
    print "\n*** Starting %s at %s"%(DB_pam.__name__,Give_UTC_now())
    scrunched_id, intermediate_ids = DB_pam(psrfits_id,proc_id,pipeline_id,DBcursor,DBconn)
    #DB_pam(2,proc_id,DBcursor,DBconn)
    print "*** Finished %s at %s\n"%(DB_pam.__name__,Give_UTC_now())
    print "*** %s returned scrunched_id: %s and intermediate_ids: "%(DB_injest_psrfits.__name__,scrunched_id)+",".join("%s" % val for val in intermediate_ids)

    #Run DB_pav
    print "\n*** Starting %s at %s"%(DB_pav.__name__,Give_UTC_now())
    DB_pav(intermediate_ids,DBcursor,DBconn)
    print "*** Finished %s at %s\n"%(DB_pav.__name__,Give_UTC_now())

    #Run DB_pat
    print "\n*** Starting %s at %s"%(DB_pat.__name__,Give_UTC_now())
    DB_pat(std_prof_id,scrunched_id,DBcursor,DBconn)
    #DB_pat(2,2,DBcursor,DBconn)
    print "*** Finished %s at %s\n"%(DB_pat.__name__,Give_UTC_now())

    #Close DB connection
    DBconn.close()

def Fill_pipeline_table(DBcursor,DBconn):
    #Calculate md5sum of pipeline script
    MD5SUM = popen("md5sum %s"%argv[0],"r").readline().split()[0].strip()
    QUERY = "INSERT INTO pipeline (pipeline_name, pipeline_version, md5sum) VALUES ('%s','%s','%s')"%(SCRIPT_NAME,VERSION,MD5SUM)
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

def Give_UTC_now():
    utcnow = datetime.datetime.utcnow()
    return "UTC %d:%02d:%02d on %d%02d%02d"%(utcnow.hour,utcnow.minute,utcnow.second,utcnow.year,utcnow.month,utcnow.day)

def main():

    if len(argv) < 2:
        Help()

    args = Parse_command_line()

    if args.template:
        print "%s"%args.template

        #Determine path (will retrieve absolute path)
        file_path, file_name = Verify_file_path(args.template, verbose=VERBOSE)

        print "%s %s"%(file_path,file_name)

        #Parse the psredit output
#        print "Importing header information for %s"%file
#        print "This file has type: %s"%data_type
#        param_names, param_values = Parse_psrfits_header(file)

        
#    if args.full_pipeline and args.std_prof_id:
#        print "###################################################"
#        print "Starting EPTA Timing Pipeline Version %.2f"%VERSION
#        proc_id = Make_Proc_ID()
#        print "Proc ID (UTC start datetime): %s"%proc_id
#        print "Start time: %s"%Give_UTC_now()
#        print "###################################################"

#        file = args.full_pipeline[0]
#        std_prof_id = args.std_prof_id[0]
#        file_path, file_name = Verify_file_path(file, verbose=VERBOSE)
#        print "Running on %s"%os.path.join(file_path, file_name)
#        Run_EPTA_pipeline(file,proc_id,std_prof_id)

#        print "###################################################"
#        print "Finished EPTA Timing Pipeline Version %.2f"%VERSION
#        print "End time: %s"%Give_UTC_now()
#        print "###################################################"
#    elif args.std_prof:
#        std_prof = args.std_prof[0]
#        print "########################################################"
#        print "Uploading a standard profile %s to the EPTA DB"%std_prof
#        proc_id = Make_Proc_ID()
#        print "Proc ID (UTC start datetime): %s"%proc_id
#        print "Start time: %s"%Give_UTC_now()
#        print "########################################################"
        
        #Make DB connection
#        DBcursor, DBconn = DBconnect(DB_HOST,DB_NAME,DB_USER,DB_PASS)

        #Fill pipeline table
#        pipeline_id = Fill_pipeline_table(DBcursor,DBconn)

#        psrfits_id, par_id = DB_injest_psrfits(std_prof,'std_prof',proc_id,DBcursor,DBconn,verbose=VERBOSE)

        #Close DB connection
#        DBconn.close()

#        print "########################################################"
#        print "Successfully uploaded standard profile %s to the EPTA DB"%std_prof
#        print "This standard profile has psrfits_id = %d"%psrfits_id
#        print "End time: %s"%Give_UTC_now()
#        print "########################################################"
    else:
        print "\nYou haven't specified a valid set of command line options.  Exiting..."
        Help()
        
main()
