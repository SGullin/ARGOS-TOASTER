#!/usr/bin/env python
""" The data reduction pipeline. This script will archive files, 
    register file-info into the database, reduce data, and store
    TOAs and processing information into the DB.
"""
import sys
import os
import os.path
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

def get_master_parfile_id(rawfile_id, existdb=None):
    """Given a rawfile_id, get the corresponding
        master_parfile_id from the database.

        Inputs:
            rawfile_id: The raw file's ID number.
            existdb: An existing database connection object.
                (Default: establish a new DB connection)

        Outputs:
            master_parfile_id: The corresponding ID of the 
                rawfile's master parfile. (None if there is
                no appropriate master parfile).
    """
    db = existdb or database.Database()
    db.connect()

    # Get ID numbers for master parfile and master template
    select = db.select([db.master_parfiles.c.parfile_id, \
                        db.master_parfiles.c.pulsar_id]).\
                where((db.rawfiles.c.pulsar_id == \
                            db.master_parfiles.c.pulsar_id) & \
                      (db.rawfiles.c.rawfile_id==rawfile_id))
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        db.close()
    if len(rows) > 1:
        raise errors.InconsistentDatabaseError("There are multiple (%d) " \
                            "master parfiles for the given pulsar_id (%d)." % \
                            (len(rows), rows[0]['pulsar_id']))
    elif len(rows) == 0:
        warnings.warn("There is no master parfile for the given " \
                        "raw file (ID: %d). Returning nothing..." % \
                        rawfile_id, errors.EptaPipelineError)
        return None
    else:
        return rows[0]['parfile_id']


def get_master_template_id(rawfile_id, existdb=None):
    """Given a rawfile_id, get the corresponding
        master_template_id from the database.

        Inputs:
            rawfile_id: The raw file's ID number.
            existdb: An existing database connection object.
                (Default: establish a new DB connection)

        Outputs:
            master_template_id: The corresponding ID of the 
                rawfile's master template. (None if there is
                no appropriate master template).
    """
    db = existdb or database.Database()
    db.connect()

    # Get ID numbers for master template and master template
    select = db.select([db.master_templates.c.template_id, \
                        db.master_templates.c.pulsar_id, \
                        db.master_templates.c.obssystem_id]).\
                where((db.rawfiles.c.pulsar_id == \
                            db.master_templates.c.pulsar_id) & \
                      (db.rawfiles.c.obssystem_id == \
                            db.master_templates.c.obssystem_id) & \
                      (db.rawfiles.c.rawfile_id==rawfile_id))
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if not existdb:
        db.close()
    if len(rows) > 1:
        raise errors.InconsistentDatabaseError("There are multiple (%d) " \
                            "master templates for the given pulsar_id (%d), " \
                            "obssystem_id (%d) combination." % \
                            (len(rows), rows[0]['pulsar_id'], 
                                rows[0]['obssystem_id']))
    elif len(rows) == 0:
        warnings.warn("There is no master template for the given " \
                        "raw file (ID: %d). Returning nothing..." % \
                        rawfile_id, errors.EptaPipelineError)
        return None
    else:
        return rows[0]['template_id']


def fill_process_table(version_id, rawfile_id, parfile_id, template_id, \
                            cmdline, nchan, nsub, existdb=None):
    db = existdb or database.Database()
    db.connect()

    ins = db.process.insert()
    values = {'version_id':version_id, \
              'rawfile_id':rawfile_id, \
              'input_args':cmdline, \
              'parfile_id':parfile_id, \
              'template_id':template_id, \
              'nchan':nchan, \
              'nsub':nsub, \
              'toa_fitting_method':config.toa_fitting_method, \
              'user_id':epu.get_current_users_id(db)}
    result = db.execute(ins, values)
    process_id = result.inserted_primary_key[0]
    result.close()
    epu.print_info("Added processing run to DB. Processing ID: %d" % \
                        process_id, 1)
    # Close DB connection
    if not existdb:
        db.close()
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
    
    db = existdb or database.Database()
    db.connect()

    try:
        trans = db.begin() # Open a transaction

        # Get version ID
        version_id = epu.get_version_id(db)
        # Get raw data from rawfile_id and verify MD5SUM
        rawfile = epu.get_rawfile_from_id(rawfile_id, db, verify_md5=True)
        # Get ephemeris from parfile_id and verify MD5SUM
        parfile = epu.get_parfile_from_id(parfile_id, db, verify_md5=True)
 
        # Manipulate the raw file
        epu.print_info("Manipulating file", 0)
        # Create a temporary file for the adjusted results
        tmpfile, adjustfn = tempfile.mkstemp()
        os.close(tmpfile)
        # Re-install ephemeris
        shutil.copy(rawfile, adjustfn)
        cmd = "pam -m -E %s --update_dm %s" % (parfile, adjustfn)
        epu.execute(cmd)
        
        # Create a temporary file for the manipulated results
        tmpfile, manipfn = tempfile.mkstemp()
        os.close(tmpfile)
        # Run the manipulator
        prepped_manipfunc([adjustfn], manipfn)
 
        # Get template from template_id and verify MD5SUM
        template = epu.get_template_from_id(template_id, db, verify_md5=True)
        
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
        
        # Read some header values from the manipulated archive
        hdr = epu.get_header_vals(manipfn, ['nchan', 'nsub', 'name', \
                                            'intmjd', 'fracmjd'])
        hdr['secs'] = int(hdr['fracmjd']*24*3600+0.5) # Add 0.5 so result is 
                                                      # rounded to nearest int
 
        # Fill pipeline table
        cmdline = " ".join(sys.argv)
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
        basefn = "%(name)s_%(intmjd)05d_%(secs)05d" % hdr

        values = [] 
        for ii, toa_id in enumerate(toa_ids):
            outfn = basefn+"_procid%d.TOA%d.png" % (process_id, ii+1)
            if ii == 0:
                fn = toadiagfn
            else:
                fn = "%s_%d" % (toadiagfn, ii+1)
            shutil.move(fn, os.path.join(diagdir, outfn))
            ins = db.toa_diagnostic_plots.insert()
            values.append({'toa_id':toa_id, \
                            'filename':outfn, \
                            'filepath':diagdir, \
                            'plot_type':'Prof-Temp Resids'})
        result = db.execute(ins, values)
        result.close()
        epu.print_info("Inserted %d TOA diagnostic plots." % len(toa_ids), 2)

        # Load processing diagnostics
        values = []
        for diagtype, diagpath in diagfns.iteritems():
            dir, fn = os.path.split(diagpath)
            ins = db.proc_diagnostic_plots.insert()
            values.append({'process_id':process_id, \
                      'filename': fn, \
                      'filepath': dir, \
                      'plot_type':diagtype})
            epu.print_info("Inserting processing diagnostic plot (type: %s)." % \
                        diagtype, 2)
        result = db.execute(ins, values)
        result.close()
    except:
        db.rollback()
        sys.stdout.write(colour.cstring("Error encountered. " \
                            "Rolling back DB transaction!\n", 'error'))
        raise
    else:
        # No exceptions encountered
        # Commit database transaction
        if not existdb:
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
    if args.rawfile is not None:
        epu.print_info("Loading rawfile %s" % args.rawfile, 1)
        args.rawfile_id = load_rawfile.load_rawfile(args.rawfile)

    if args.parfile is not None:
        epu.print_info("Loading parfile %s" % args.parfile, 1)
        args.parfile_id = load_parfile.load_parfile(args.parfile)
        
    if args.template is not None:
        epu.print_info("Loading template %s" % args.template, 1)
        args.template_id = load_template.load_template(args.template)

    if args.parfile_id is None:
        args.parfile_id = get_master_parfile_id(args.rawfile_id)
        if args.parfile_id is None:
            raise errors.NoMasterError("A master parfile is required " \
                                    "in the database if no parfile is " \
                                    "provided on the command line.")

    if args.template_id is None:
        args.template_id = get_master_template_id(args.rawfile_id)
        if args.template_id is None:
            raise errors.NoMasterError("A master template is required " \
                                    "in the database if no template is " \
                                    "provided on the command line.")

    epu.print_info("Using the following IDs:\n" \
                     "    rawfile_id: %d\n" \
                     "    parfile_id: %d\n" \
                     "    template_id: %d" % \
                     (args.rawfile_id, args.parfile_id, args.template_id), 1)
    
    manip_kwargs = manipulators.extract_manipulator_arguments(args.manipfunc, \
                                                            args)
    prepped_manipfunc = manipulators.prepare_manipulator(args.manipfunc, \
                                                            manip_kwargs)
    
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
    parentparser = argparse.ArgumentParser(add_help=False)
    # Raw data
    rawgroup = parentparser.add_mutually_exclusive_group(required=True)
    rawgroup.add_argument("rawfile", nargs='?', type=str, default=None, \
                        help="A raw file to archive/load to DB and " \
                            "generate TOAs for.")
    rawgroup.add_argument('-r', '--rawfile-id', dest='rawfile_id', \
                        type=int, default=None, \
                        help="ID of an already archived/loaded raw data " \
                            "file to use for running the full pipeline.")
    # Ephemeris
    pargroup = parentparser.add_mutually_exclusive_group(required=False)
    pargroup.add_argument('-p', '--parfile-id', dest='parfile_id', \
                        type=int, default=None, \
                        help="ID of ephemeris to use for running the " \
                            "full pipeline.")
    pargroup.add_argument('--parfile', dest='parfile', type=str, \
                        default=None,
                        help="A parfile to archive/load to DB and " \
                            "use when generating TOAs.")
    # Template profile
    tmpgroup = parentparser.add_mutually_exclusive_group(required=False)
    tmpgroup.add_argument('-t', '--template-id', dest='template_id',
                        type=int, default=None, \
                        help="ID of template profile to use for running " \
                            "the full pipeline.")
    tmpgroup.add_argument('--template', dest='template', type=str, \
                        default=None,
                        help="A template to archive/load to DB and use " \
                            "when generating TOAs.")
    mainparser = epu.DefaultArguments(prog='epta_pipeline', \
                            description='Reduce an already-uploaded ' \
                                'archive. Both a pre-loaded parfile, and a ' \
                                'pre-loaded template must be provided as well. ' \
                                'TOAs generated are loaded into the database, ' \
                                'as is information about the processing run.')

    subparsers = mainparser.add_subparsers(dest='manipulator', \
                            title="Manipulators", \
                            description="The function used to manipulate " \
                                "rawfiles before generating TOAs. Note: the " \
                                "number of TOAs is (#subbands * #subints) in " \
                                "the manipulated file.")
    for name in manipulators.registered_manipulators:
        m = manipulators.__dict__[name]
        m_parser = subparsers.add_parser(m.plugin_name, help=m.__doc__, \
                description="%s (The options listed below are " \
                            "'%s'-specific.)" % (m.__doc__, name), \
                parents=[parentparser])
        m.add_arguments(m_parser)
        m_parser.set_defaults(manipfunc=m.manipulate)
        m_parser.add_standard_group()
        m_parser.add_debug_group()

    args=mainparser.parse_args()
    main()
