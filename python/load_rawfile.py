#!/usr/bin/python2.6
####################
# load_rawfile.py #
VERSION = 0.1
####################

#Imported modules
import sys
import os
import os.path
import glob
import shutil

# import pipeline utilities
import epta_pipeline_utils as epu
import config
import errors

# Global definitions
userid_cache = None
pulsarid_cache = None
obssystemid_cache = None


def Help():
    print "\nLoad raw files to database"
    print "Version: %.2f"%(VERSION)+"\n"
    print ("'%s' accepts only raw files. Wildcard allowed.\n")% sys.argv[0]
    sys.exit(0)

def get_userid():
    """Return user_id for the current user.

        Inputs:
            None

        Output:
            userid: The user_id as taken from the DB.
    """
    global userid_cache
    if userid_cache is None:
        userid_cache = epu.get_userids()

    uname = os.getlogin()
    if uname in userid_cache:
        id = userid_cache[uname]
    else:
        raise errors.UnrecognizedValueError("The user name %s " \
                                            "is not recognized!" % uname)
    return id


def get_pulsarid(psrname):
    """Return pulsar_id for the given pulsar name.

        Inputs:
            psrname: A pulsar name

        Output:
            pulsarid: The pulsar_id as taken from the DB.
    """
    global pulsarid_cache
    if pulsarid_cache is None:
        pulsarid_cache = epu.get_pulsarids()

    if psrname in pulsarid_cache:
        id = pulsarid_cache[psrname]
    else:
        raise errors.UnrecognizedValueError("The pulsar name %s " \
                                            "is not recognized!" % psrname)
    return id


def get_obssystemid(telescope, frontend, backend):
    """Return obssystem_id for the given telescope, frontend, 
        backend combination.

        Inputs:
            telescope: The standard telescope name.
                (ie one of WSRT, SRT, Nancay, Jodrell, Effelsberg)
            frontend: The frontend name.
            backend: The backend name.

        Output:
            obssystemid: The obssystem_id as taken from the DB.
    """
    global obssystemid_cache
    if obssystemid_cache is None:
        obssystemid_cache = epu.get_obssystemids()

    if (telescope, frontend, backend) in obssystemid_cache:
        id = obssystemid_cache[(telescope, frontend, backend)]
    else:
        raise errors.UnrecognizedValueError("There are no DB entries with " \
                                        "telescope='%s', frontend='%s' and " \
                                        "backend='%s' in 'obssystems' table" % \
                                        (telescope, frontend, backend))
    return id
    
                                        
def populate_rawfiles_table(fn, params, DBcursor):
    # md5sum helper function in epu
    md5 = epu.Get_md5sum(fn);
    path, fn = os.path.split(os.path.abspath(fn))
    
    # Does this file exist already?
    query = "SELECT rawfile_id FROM rawfiles WHERE md5sum = '%s'" % md5
    DBcursor.execute(query)
    rows = DBcursor.fetchall()
    if rows:
        raise DatabaseError("Rawfile with MD5 (%s) in database already" % md5)
    
    # Insert the file
    query = "INSERT INTO rawfiles " + \
            "SET md5sum = '%s', " % md5 + \
                "filename = '%s', " % fn + \
                "filepath = '%s', " % path + \
                "user_id = '%s', " % params['user_id'] + \
                "add_time = NOW(), " + \
                "pulsar_id = '%s', " % params['pulsar_id'] + \
                "obssystem_id = '%s', " % params['obssystem_id'] + \
                "nbin = %d, " % int(params['nbin']) + \
                "nchan = %d, " % int(params['nchan']) + \
                "npol = %d, " % int(params['npol']) + \
                "nsub = %d, " % int(params['nsubint']) + \
                "type = '%s', " % params['type'] + \
                "site = '%s', " % params['site'] + \
                "name = '%s', " % params['name'] + \
                "coord = '%s', " % params['coord'] + \
                "freq = %.15g, " % float(params['freq']) + \
                "bw = %.15g, " % float(params['bw']) + \
                "dm = %.15g, " % float(params['dm']) + \
                "rm = %.15g, " % float(params['rm']) + \
                "dmc = %.15g, " % float(params['dmc']) + \
                "rmc = %.15g, " % float(params['rmc']) + \
                "polc = %.15g, " % float(params['polc']) + \
                "scale = '%s', " % params['scale'] + \
                "state = '%s', " % params['state'] + \
                "length = %.15g, " % float(params['length']) + \
                "rcvr_name = '%s', " % params['rcvr:name'] + \
                "rcvr_basis = '%s', " % params['rcvr:basis'] + \
                "be_name = '%s'" % params['be:name'] 
    DBcursor.execute(query)
    
    # Get the rawfile_id of the file that was just entered
    query = "SELECT LAST_INSERT_ID()"
    DBcursor.execute(query)
    rawfile_id = DBcursor.fetchone()[0]
    return rawfile_id


def create_diagnostics(rawfile_ids,DBcursor,DBconn):
    for rawfile_id in rawfile_ids:
        query = "select filepath,filename from rawfiles where datatype='intermediate' and rawfile_id='%i'"%rawfile_id
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
            elif file_ext == "Ft":
                command = "pav -Y %s -g %s.png/png"%(file,file)
            elif file_ext == "FT":
                command = "pav -S %s -g %s.png/png"%(file,file)
            elif file_ext == "FTp":
                command = "pav -DFTp %s -g%s.png/png"%(file,file)
            epu.execute(command)


def move_file(file, destdir):
    srcdir, fn = os.path.split(file)
    dest = os.path.join(destdir, fn)

    # Check if the directory exists
    # If not, create it
    if not os.path.isdir(destdir):
        os.makedirs(destdir)

    # Check that our file doesn't already exist in 'dest'
    # If it does exist do nothing but print a warning
    if not os.path.isfile(dest):
        # Copy file to 'dest'
        shutil.move(file, dest)
    elif destdir == srcdir:
        # File is already located in its destination
        # Do nothing
        pass
    else:
        # Another file with the same name is the destination directory
        # Compare the files
        srcmd5 = epu.Get_md5sum(file)
        srcsize = os.path.getsize(file)
        destmd5 = epu.Get_md5sum(dest)
        destsize = os.path.getsize(dest)
        if (srcmd5==destmd5) and (srcsize==destsize):
            # Files are the same, so remove src as if we moved it
            # (taking credit for work that was already done...)
            os.remove(file)
        else:
            # The files are not the same! This is not good.
            # Raise an exception.
            raise errors.FileError("File (%s) cannot be archived. " \
                    "There is already a file archived by that name " \
                    "in the appropriate archive location (%s), but " \
                    "the two files are _not_ identical. " \
                    "(source: MD5=%s, size=%d bytes; dest: MD5=%s, " \
                    "size=%d bytes)" % \
                    (file, dest, srcmd5, srcsize, destmd5, destsize))

    # Change permissions so the file can no longer be written to
    os.chmod(dest, 0440) # "0440" is an integer in base 8. It works
                         # the same way 440 does for chmod on cmdline
    return dest


def prep_file(fn):
    """Prepare file for archiving/loading.
        
        Also, perform some checks on the file to make sure we
        won't run into problems later. Checks peformed:
            - Existence of file.
            - Read/write access for file (so it can be moved).
            - Header contains all necessary values.
            - Site/observing system is recognized.

        Input:
            fn: The name of the file to check.

        Outputs:
            params: A dictionary of info to be uploaded.
    """
    # Check existence of file
    epu.Verify_file_path(fn)

    # Check file permissions allow for writing and reading
    if not os.access(fn, os.W_OK | os.R_OK):
        raise errors.FileError("File (%s) is not read/writable!" % fn)

    # Grab header info
    hdritems = ["nbin", "nchan", "npol", "nsubint", "type", "site", \
         	"name", "type", "coord", "freq", "bw", "dm", "rm", \
      	        "dmc", "rmc", "polc", "scale", "state", "length", \
    	        "rcvr:name", "rcvr:basis", "be:name"]
    params = epu.parse_psrfits_header(fn, hdritems)

    # Get telescope name
    params['telescope'] = epu.get_telescope(params['site'])

    # Check if obssystem_id, pulsar_id, user_id can be found
    params['obssystem_id'] = get_obssystemid(params['telescope'], \
                                params['rcvr:name'], params['be:name'])
    params['pulsar_id'] = get_pulsarid(params['name'])
    params['user_id'] = get_userid()
    return params


def main():
    # Collect input files
    infiles = set(args.infiles)
    for glob_expr in args.glob_exprs:
        infiles.update(glob.glob(glob_expr))
    infiles = list(infiles)

    if not infiles:
        sys.stderr.write("You didn't provide any files to load. " \
                         "You should consider including some next time...\n")
        sys.exit(1)
    # Create DB connection instance
    DBcursor, DBconn = epu.DBconnect()
    
    try:
        # Enter information in rawfiles table
        # create diagnostic plots and metrics.
        # Also fill-in raw_diagnostics and raw_diagnostic_plots tables
        for fn in infiles:
            try:
                if config.verbosity:
                    print "Working on %s (%s)" % (fn, epu.Give_UTC_now())
                # Check the file and parse the header
                params = prep_file(fn)
                
                # Move the File
                destdir = epu.get_archive_dir(fn, site=params['site'], \
                            backend=params['be:name'], psrname=params['name'])
                newfn = move_file(fn, destdir)
                
                if config.verbosity:
                    print "%s moved to %s (%s)" % (fn, newfn, epu.Give_UTC_now())

                # Register the file into the database
                rawfile_id = populate_rawfiles_table(newfn, params, DBcursor)
                
                if config.verbosity:
                    print "Finished with %s - rawfile_id=%d (%s)" % \
                        (fn, rawfile_id, epu.Give_UTC_now())

                # Create diagnostic plots and load them into the DB
                #create_diagnostics(rawfile_id,DBcursor,DBconn)
            except errors.EptaPipelineError, msg:
                sys.stderr.write("%s\nSkipping...\n" % msg)
    finally:
        # Close DB connection
        DBconn.close()


if __name__=='__main__':
    parser = epu.DefaultArguments(description="Archive raw files, " \
                                        "and load their info into the database.")
    parser.add_argument("infiles", nargs='*', action='store', \
                        help="Files with headers to correct.")
    parser.add_argument("-g", "--glob-files", action="append", \
                        dest='glob_exprs', default=[], \
                        help="Glob expression identifying files with " \
                             "headers to correct. Be sure to correctly " \
                             "quote the expression. Multiple -g/--glob-files " \
                             "options can be provided.")
    args = parser.parse_args()
    main()

