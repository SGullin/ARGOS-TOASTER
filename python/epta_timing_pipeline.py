#!/usr/bin/python
##################################
# epta_timing_pipeline.py 
PIPE_NAME = "epta_timing_pipeline"
VERSION = 0.2
##################################

#Imported modules
import sys
import os
import os.path
import datetime
import argparse
import warnings
import tempfile
import shutil

import colour
import errors
import manipulators
import database
import load_rawfile
import load_parfile
import load_template
import epta_pipeline_utils as epu
import config

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
    sys.exit(0)

def Parse_command_line():
    parentparser = argparse.ArgumentParser(add_help=False)
    # Raw data
    rawgroup = parentparser.add_mutually_exclusive_group(required=True)
    # By file name
    rawgroup.add_argument("rawfile",
                        nargs='?',
                        type=str,
                        default=None,
                        help="A raw file to archive/load to DB and generate TOAs for.")
    # Or by rawfile ID
    rawgroup.add_argument('-r', '--rawfile-id',
                        dest='rawfile_id',
                        type=int,
                        default=None,
                        help="ID of an already archived/loaded raw data file to use for " \
                                "running the full pipeline.")
    #Ephemeris
    pargroup = parentparser.add_mutually_exclusive_group(required=False)
    pargroup.add_argument('-p', '--parfile-id',
                        dest='parfile_id', 
                        type=int,
                        default=None,
                        help="ID of ephemeris to use for running the full pipeline.")
    pargroup.add_argument('--parfile', 
                        dest='parfile',
                        type=str,
                        default=None,
                        help="A parfile to archive/load to DB and use when generating TOAs.")
    #Template profile
    tmpgroup = parentparser.add_mutually_exclusive_group(required=False)
    tmpgroup.add_argument('-t', '--template-id',
                        dest='template_id',
                        type=int,
                        default=None,
                        help="ID of template profile to use for running the full pipeline.")
    tmpgroup.add_argument('--template',
                        dest='template',
                        type=str,
                        default=None,
                        help="A template to archive/load to DB and use when generating TOAs.")
    
    mainparser = epu.DefaultArguments(
        prog='epta_pipeline',
        description='Reduce an already-uploaded archive. ' \
                    'Both a pre-loaded parfile, and a pre-loaded template ' \
                    'must be provided as well. TOAs generated are loaded ' \
                    'into the database, as is information about the ' \
                    'processing run.')

    subparsers = mainparser.add_subparsers(dest='manipulator', \
                            title="Manipulators", \
                            description="The function used to manipulate " \
                                "rawfiles before generating TOAs. Note: the " \
                                "number of TOAs is (#subbands * #subints) in " \
                                "the manipulated file.")
    for name in manipulators.registered_manipulators:
        m = manipulators.__dict__[name]
        m_parser = subparsers.add_parser(m.plugin_name, help=m.__doc__, \
                description="%s (The options listed below are '%s'-specific.)" % \
                            (m.__doc__, name), \
                parents=[parentparser])
        m.add_arguments(m_parser)
        m_parser.set_defaults(manipfunc=m.manipulate)
        m_parser.add_standard_group()
        m_parser.add_debug_group()

    args=mainparser.parse_args()
    return args


def get_master_ids(rawfile_id, existdb=None):
    """Given a rawfile_id, fetch the corresponding
        master_template_id and master_parfile_id from
        the database and return them.

        Inputs:
            rawfile_id: The raw file's ID number.
            existdb: An existing database connection object.
                (Default: establish a new DB connection)

        Outputs:
            master_template_id: The corresponding ID of the 
                rawfile's master template. (None if there is
                no appropriate master template).
            master_parfile_id: The corresponding ID of the 
                rawfile's master parfile. (None if there is
                no appropriate master parfile).
    """
    if existdb:
        db = existdb
    else:
        db = database.Database()

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
    master_template_id, master_parfile_id = db.fetchone()
    
    if master_template_id is None:
        raise errors.NoMasterError("No master template found for " \
                                    "rawfile (ID: %d)" % rawfile_id)
    if master_parfile_id is None:
        raise errors.NoMasterError("No master parfile found for " \
                                    "rawfile (ID: %d)" % rawfile_id)

    if not existdb:
        db.close()

    return master_template_id, master_parfile_id


def fill_process_table(version_id, rawfile_id, parfile_id, template_id, \
                            cmdline, nchan, nsub, db):
    query = "INSERT INTO process " \
                "(version_id, " \
                "rawfile_id, " \
                "add_time, " \
                "input_args, " \
                "parfile_id, " \
                "template_id, " \
                "nchan, " \
                "nsub, " \
                "toa_fitting_method, " \
                "dm, " \
                "user_id) " \
            "SELECT %d, " % version_id + \
                "%d, " % rawfile_id + \
                "NOW(), " + \
                "'%s', " % cmdline + \
                "par.parfile_id, " + \
                "%d, " % template_id + \
                "%d, " % nchan + \
                "%d, " % nsub + \
                "'%s', " % config.toa_fitting_method + \
                "par.dm, " \
                "%d " % epu.get_current_users_id(db) + \
            "FROM parfiles AS par " \
            "WHERE par.parfile_id = %d" % parfile_id
    db.execute(query)
    query = "SELECT LAST_INSERT_ID()"
    db.execute(query)
    process_id = db.fetchone()[0]
    epu.print_info("Added processing run to DB. Processing ID: %d" % \
                        process_id, 1)
    return process_id
    

def pipeline_core(manip_name, prepped_manipfunc, \
                        rawfile_id, parfile_id, template_id, \
                        existdb=None):
    """Run a prepared manipulator function on the raw file with 
        ID 'rawfile_id'. Then generate TOAs and load them into the DB.

        Inputs:
            manip_name: The name of the manipulator being used.
            prepped_manipfunc: A prepared manipulator function.
            rawfile_id: The ID number of the raw file to generate TOAs from.
            parfile_id: The ID number of the par file to install into the
                raw file.
            tempalte_id: The ID number of the template to use.
            existdb: An existing database connection object.
                (Default: establish a new DB connection)

        Outputs:
            None
    """
    #Start pipeline
    print "###################################################"
    print "Starting EPTA Timing Pipeline"
    print "Start time: %s"%epu.Give_UTC_now()
    print "###################################################"
    
    if existdb:
        db = existdb
    else:
        db = database.Database()

    try:
        # Get version ID
        version_id = epu.get_version_id(db)
 
        #Get raw data from rawfile_id and verify MD5SUM
        rawfile = epu.get_file_from_id('rawfile', rawfile_id, db)
            
        #Get ephemeris from parfile_id and verify MD5SUM
        parfile = epu.get_file_from_id('parfile', parfile_id, db)
 
        # Manipulate the raw file
        epu.print_info("Manipulating file", 0)
        # Create a temporary file for the results
        tmpfile, manipfn = tempfile.mkstemp()
        os.close(tmpfile)
        # Run the manipulator
        prepped_manipfunc([rawfile], manipfn)
 
        #Get template from template_id and verify MD5SUM
        template = epu.get_file_from_id('template', template_id, db)
        
        # Create a temporary file for the toa diagnostic plots
        tmpfile, toadiagfn = tempfile.mkstemp()
        os.close(tmpfile)
        # Generate TOAs with pat
        epu.print_info("Computing TOAs", 0)
        stdout, stderr = epu.execute("pat -f tempo2 -A %s -s %s " \
                                        "-t -K %s/PNG %s" % \
                    (config.toa_fitting_method, template, toadiagfn, manipfn))
 
        # Check version ID is still the same. Just in case.
        new_version_id = epu.get_version_id(db)
        if version_id != new_version_id:
            raise errors.EptaPipelineError("Weird... Version ID at the start " \
                                            "of processing (%s) is different " \
                                            "from at the end (%d)!" % \
                                            (version_id, new_version_id))
 
        # Fill pipeline table
        cmdline = " ".join(sys.argv)
        hdr = epu.get_header_vals(manipfn, ['nchan', 'nsub'])
        process_id = fill_process_table(version_id, rawfile_id, parfile_id, \
                            template_id, cmdline, hdr['nchan'], hdr['nsub'], db)
        
        # Insert TOAs into DB
        toa_ids = []
        for toastr in stdout.split("\n"):
            toastr = toastr.strip()
            if toastr and (toastr != "FORMAT 1") and \
                        (toastr != "Plotting %s" % manipfn):
                print toastr
                toa_id = epu.DB_load_TOA(toastr, process_id, \
                                            template_id, rawfile_id, db)
                toa_ids.append(toa_id)
                 
        # Create processing diagnostics
        epu.print_info("Generating proessing diagnostics", 0)
        diagdir = epu.make_proc_diagnostics_dir(manipfn, process_id)
        suffix = "_procid%d.%s" % (process_id, manip_name)
        diagfns = epu.create_datafile_diagnostic_plots(manipfn, diagdir, suffix)
       
        # Copy TOA diagnostic plots and register them into DB
        hdr = epu.get_header_vals(manipfn, ['name', 'intmjd', 'fracmjd'])
        hdr['secs'] = int(hdr['fracmjd']*24*3600+0.5) # Add 0.5 so result is 
                                                      # rounded to nearest int
        basefn = "%(name)s_%(intmjd)05d_%(secs)05d" % hdr

        for ii, toa_id in enumerate(toa_ids):
            outfn = basefn+"_procid%d.TOA%d.png" % (process_id, ii+1)
            if ii == 0:
                fn = toadiagfn
            else:
                fn = "%s_%d" % (toadiagfn, ii+1)
            shutil.move(fn, os.path.join(diagdir, outfn))
            query = "INSERT INTO toa_diagnostic_plots " + \
                    "SET toa_id=%d, " % toa_id + \
                        "filename='%s', " % outfn + \
                        "filepath='%s', " % diagdir + \
                        "plot_type='Prof-Temp Resids'"
            db.execute(query)
        epu.print_info("Inserted %d TOA diagnostic plots." % len(toa_ids), 2)

        # Load processing diagnostics
        for diagtype, diagpath in diagfns.iteritems():
            dir, fn = os.path.split(diagpath)
            query = "INSERT INTO proc_diagnostic_plots " + \
                    "SET process_id=%d, " % process_id + \
                        "filename='%s', " % fn + \
                        "filepath='%s', " % dir + \
                        "plot_type='%s'" % diagtype
            db.execute(query)
            epu.print_info("Inserted processing diagnostic plot (type: %s)." % \
                        diagtype, 2)
    except:
        db.rollback()
        sys.stdout.write(colour.cstring("Error encountered. " \
                            "Rolling back DB transaction!\n", 'error'))
        raise
    else:
        # No exceptions encountered
        # Commit database transaction
        db.commit()
    finally:
        #End pipeline
        print "###################################################"
        print "Finished EPTA Timing Pipeline"
        print "End time: %s" % epu.Give_UTC_now()
        print "###################################################"    
        
        # Close DB connection
        if not existdb:
            db.close()


def main():
    #Exit if there are no or insufficient arguments
    if not len(sys.argv):
        Help()

    args = Parse_command_line()

    if args.rawfile is not None:
        epu.print_info("Loading rawfile %s" % args.rawfile, 1)
        args.rawfile_id = load_rawfile.load_rawfile(args.rawfile)

    if args.parfile is not None:
        epu.print_info("Loading parfile %s" % args.parfile, 1)
        args.parfile_id = load_parfile.load_parfile(args.parfile)
        
    if args.template is not None:
        epu.print_info("Loading template %s" % args.template, 1)
        args.template_id = load_template.load_template(args.template)

    if (args.parfile_id is None) or (args.template_id is None):
        # Get master parfile and template IDs together to reduce number of
        # DB queries required.
        master_template_id, master_parfile_id = get_master_ids(args.rawfile_id)
        if args.parfile_id is None:
            args.parfile_id = master_parfile_id
        if args.template_id is None:
            args.template_id = master_template_id

    epu.print_info("Using the following IDs:\n" \
                     "    rawfile_id: %d\n" \
                     "    parfile_id: %d\n" \
                     "    template_id: %d" % \
                     (args.rawfile_id, args.parfile_id, args.template_id), 1)
    
    manip_kwargs = manipulators.extract_manipulator_arguments(args.manipfunc, args)
    prepped_manipfunc = manipulators.prepare_manipulator(args.manipfunc, manip_kwargs)
    
    if len(manip_kwargs):
        manip_arglist = []
        for key in sorted(manip_kwargs.keys()):
            manip_arglist.append("%s = %s" % (key, manip_kwargs[key]))
        manip_argstr = "\n    ".join(manip_arglist)
    else:
        manip_argstr = "None"

    epu.print_info("Manipuator being used: %s\n" \
                    "Arguments provided:\n" \
                    "    %s" % (args.manipulator, manip_argstr), 1) 
              
    
    # Run pipeline core
    pipeline_core(args.manipulator, prepped_manipfunc, \
                    args.rawfile_id, args.parfile_id, args.template_id)


if __name__ == "__main__":
    main()
