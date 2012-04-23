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
import warnings

# import pipeline utilities
import epta_pipeline_utils as epu
import config
import errors
import database

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
    
                                        
def populate_rawfiles_table(db, fn, params):
    # md5sum helper function in epu
    md5 = epu.Get_md5sum(fn);
    path, fn = os.path.split(os.path.abspath(fn))
    
    # Does this file exist already?
    query = "SELECT rawfile_id, pulsar_id " \
            "FROM rawfiles " \
            "WHERE md5sum = '%s'" % md5
    db.execute(query)
    rows = db.fetchall()
    if len(rows) > 1:
        raise errors.InconsistentDatabaseError("There are %d rawfiles " \
                    "with MD5 (%s) in the database already" % (len(rows), md5))
    elif len(rows) == 1:
        rawfile_id, psr_id = rows[0]
        if psr_id == params['pulsar_id']:
            warnings.warn("A rawfile with this MD5 (%s) already exists " \
                            "in the DB for this pulsar (ID: %d). " \
                            "Doing nothing..." % (md5, psr_id), \
                            errors.EptaPipelineWarning)
        else:
            raise errors.InconsistentDatabaseError("A rawfile with this " \
                            "MD5 (%s) already exists in the DB, but for " \
                            "a different pulsar (ID: %d)!" % (md5, psr_id))
    else:
        # Based on its MD5, this rawfile doesn't already 
        # exist in the DB. Insert it.

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
                    "nsub = %d, " % int(params['nsub']) + \
                    "type = '%s', " % params['type'] + \
                    "site = '%s', " % params['telescop'] + \
                    "name = '%s', " % params['name'] + \
                    "coord = '%s,%s', " % (params['ra'],params['dec']) + \
                    "freq = %.15g, " % float(params['freq']) + \
                    "bw = %.15g, " % float(params['bw']) + \
                    "dm = %.15g, " % float(params['dm']) + \
                    "rm = %.15g, " % float(params['rm']) + \
                    "dmc = %.15g, " % float(params['dmc']) + \
                    "rmc = %.15g, " % float(params['rm_c']) + \
                    "polc = %.15g, " % float(params['pol_c']) + \
                    "scale = '%s', " % params['scale'] + \
                    "state = '%s', " % params['state'] + \
                    "length = %.15g, " % float(params['length']) + \
                    "rcvr_name = '%s', " % params['rcvr'] + \
                    "rcvr_basis = '%s', " % params['basis'] + \
                    "be_name = '%s'" % params['backend'] 
        db.execute(query)
        
        # Get the rawfile_id of the file that was just entered
        query = "SELECT LAST_INSERT_ID()"
        db.execute(query)
        rawfile_id = db.fetchone()[0]
    return rawfile_id


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

    # Connect to the database
    db = database.Database()

    try:
        # Enter information in rawfiles table
        # create diagnostic plots and metrics.
        # Also fill-in raw_diagnostics and raw_diagnostic_plots tables
        for fn in infiles:
            try:
                if config.verbosity:
                    print "Working on %s (%s)" % (fn, epu.Give_UTC_now())
                # Check the file and parse the header
                params = epu.prep_file(fn)
                
                # Move the File
                destdir = epu.get_archive_dir(fn, site=params['telescop'], \
                            backend=params['backend'], \
                            receiver=params['rcvr'], \
                            psrname=params['name'])
                newfn = epu.archive_file(fn, destdir)
                
                if config.verbosity:
                    print "%s moved to %s (%s)" % (fn, newfn, epu.Give_UTC_now())

                # Register the file into the database
                rawfile_id = populate_rawfiles_table(db, newfn, params)
                
                if config.verbosity:
                    print "Finished with %s - rawfile_id=%d (%s)" % \
                        (fn, rawfile_id, epu.Give_UTC_now())

                # TODO: Create diagnostic plots and load them into the DB
            
            except errors.EptaPipelineError, msg:
                sys.stderr.write("%s\nSkipping...\n" % msg)
    finally:
        # Close DB connection
        db.close()


if __name__=='__main__':
    parser = epu.DefaultArguments(description="Archive raw files, " \
                                        "and load their info into the database.")
    parser.add_argument("infiles", nargs='*', action='store', \
                        help="Files to load into the DB")
    parser.add_argument("-g", "--glob-files", action="append", \
                        dest='glob_exprs', default=[], \
                        help="Glob expression identifying files " \
                             "to load into the DB. Be sure to correctly " \
                             "quote the expression. The -g/--glob-files " \
                             "option can be provided multiple times.")
    args = parser.parse_args()
    main()

