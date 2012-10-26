#!/usr/bin/env python
""" The data reduction pipeline. This script will archive files, 
    register file-info into the database, reduce data, and store
    TOAs and processing information into the DB.
"""
import copy
import sys
import os
import os.path
import argparse
import warnings
import tempfile
import shutil
import traceback

import colour
import errors
import manipulators
import database
import load_rawfile
import load_parfile
import load_template
import utils
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
                        rawfile_id, errors.EptaPipelineWarning)
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
                        manip, nchan, nsub, existdb=None):
    db = existdb or database.Database()
    db.connect()

    ins = db.process.insert()
    values = {'version_id':version_id, \
              'rawfile_id':rawfile_id, \
              'parfile_id':parfile_id, \
              'template_id':template_id, \
              'manipulator': manip.name, \
              'manipulator_args': manip.argstr, \
              'nchan':nchan, \
              'nsub':nsub, \
              'toa_fitting_method':config.toa_fitting_method, \
              'user_id':utils.get_userid()}
    result = db.execute(ins, values)
    process_id = result.inserted_primary_key[0]
    result.close()
    utils.print_info("Added processing run to DB. Processing ID: %d" % \
                        process_id, 1)
    # Close DB connection
    if not existdb:
        db.close()
    return process_id
    

def pipeline_core(manip, rawfile_id, parfile_id, template_id, \
                        existdb=None):
    """Run a prepared manipulator function on the raw file with 
        ID 'rawfile_id'. Then generate TOAs and load them into the DB.

        Inputs:
            manip: A manipulator instance.
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
    print "Start time: %s"%utils.Give_UTC_now()
    print "###################################################"
    
    db = existdb or database.Database()
    db.connect()

    try:
        trans = db.begin() # Open a transaction

        # Get version ID
        version_id = utils.get_version_id(db)
        # Get raw data from rawfile_id and verify MD5SUM
        rawfile = utils.get_rawfile_from_id(rawfile_id, db, verify_md5=True)
        # Get ephemeris from parfile_id and verify MD5SUM
        parfile = utils.get_parfile_from_id(parfile_id, db, verify_md5=True)
 
        # Manipulate the raw file
        utils.print_info("Manipulating file", 0)
        # Create a temporary file for the adjusted results
        tmpfile, adjustfn = tempfile.mkstemp(prefix='toaster_tmp', \
                            suffix='_newephem.ar', dir=config.base_tmp_dir)
        os.close(tmpfile)
        # Re-install ephemeris
        shutil.copy(rawfile, adjustfn)
        cmd = "pam -m -E %s --update_dm %s" % (parfile, adjustfn)
        utils.execute(cmd)
        
        # Create a temporary file for the manipulated results
        tmpfile, manipfn = tempfile.mkstemp(prefix='toaster_tmp', \
                            suffix='_manip.ar', dir=config.base_tmp_dir)
        os.close(tmpfile)
        # Run the manipulator
        manip.run([adjustfn], manipfn)
 
        # Get template from template_id and verify MD5SUM
        template = utils.get_template_from_id(template_id, db, verify_md5=True)
        
        # Create a temporary file for the toa diagnostic plots
        tmpfile, toadiagfn = tempfile.mkstemp(prefix='toaster_tmp', \
                            suffix='_TOAdiag.png', dir=config.base_tmp_dir)
        os.close(tmpfile)
        # Generate TOAs with pat
        utils.print_info("Computing TOAs", 0)
        patout, paterr = utils.execute("pat -f tempo2 -A %s -s %s " \
                                "-C 'gof length bw nbin nchan nsubint' " \
                                "-t -K %s/PNG  %s" % \
                    (config.toa_fitting_method, template, toadiagfn, manipfn))
 
        # Check version ID is still the same. Just in case.
        new_version_id = utils.get_version_id(db)
        if version_id != new_version_id:
            raise errors.EptaPipelineError("Weird... Version ID at the start " \
                                            "of processing (%s) is different " \
                                            "from at the end (%d)!" % \
                                            (version_id, new_version_id))
        
        # Read some header values from the manipulated archive
        hdr = utils.get_header_vals(manipfn, ['nchan', 'nsub', 'name', \
                                            'intmjd', 'fracmjd'])
        hdr['secs'] = int(hdr['fracmjd']*24*3600+0.5) # Add 0.5 so result is 
                                                      # rounded to nearest int
 
        # Fill pipeline table
        cmdline = " ".join(sys.argv)
        process_id = fill_process_table(version_id, rawfile_id, parfile_id, \
                                     template_id, manip, hdr['nchan'], hdr['nsub'], db)
        
        # Parse pat output
        toainfo = utils.parse_pat_output(patout)

        # Insert TOAs into DB
        toa_ids = utils.load_toas(toainfo, process_id, template_id, rawfile_id, db)
                 
        # Create processing diagnostics
        utils.print_info("Generating proessing diagnostics", 0)
        diagdir = utils.make_proc_diagnostics_dir(manipfn, process_id)
        suffix = "_procid%d.%s" % (process_id, manip.name)
        diagfns = utils.create_rawfile_diagnostic_plots(manipfn, diagdir, suffix)
       
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
        utils.print_info("Inserted %d TOA diagnostic plots." % len(toa_ids), 2)

        # Load processing diagnostics
        values = []
        for diagtype, diagpath in diagfns.iteritems():
            dir, fn = os.path.split(diagpath)
            ins = db.proc_diagnostic_plots.insert()
            values.append({'process_id':process_id, \
                      'filename': fn, \
                      'filepath': dir, \
                      'plot_type':diagtype})
            utils.print_info("Inserting processing diagnostic plot (type: %s)." % \
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
        db.commit()
    finally:
        #End pipeline
        print "###################################################"
        print "Finished EPTA Timing Pipeline"
        print "End time: %s" % utils.Give_UTC_now()
        print "###################################################"    
        
        # Close DB connection
        if not existdb:
            db.close()


def reduce_rawfile(args, leftover_args=[], existdb=None):
    if args.rawfile is not None:
        utils.print_info("Loading rawfile %s" % args.rawfile, 1)
        args.rawfile_id = load_rawfile.load_rawfile(args.rawfile, existdb)
    elif args.rawfile_id is None:
        # Neither a rawfile, nor a rawfile_id was provided
        raise errors.BadInputError("Either a rawfile, or a rawfile_id " \
                                    "_must_ be provided!")
 
    if args.parfile is not None:
        utils.print_info("Loading parfile %s" % args.parfile, 1)
        args.parfile_id = load_parfile.load_parfile(args.parfile, existdb=existdb)
        
    if args.template is not None:
        utils.print_info("Loading template %s" % args.template, 1)
        args.template_id = load_template.load_template(args.template, \
                                                        existdb=existdb)
 
    if args.parfile_id is None:
        args.parfile_id = get_master_parfile_id(args.rawfile_id, existdb=existdb)
        if args.parfile_id is None:
            raise errors.NoMasterError("A master parfile is required " \
                                    "in the database if no parfile is " \
                                    "provided on the command line.")
 
    if args.template_id is None:
        args.template_id = get_master_template_id(args.rawfile_id, existdb=existdb)
        if args.template_id is None:
            raise errors.NoMasterError("A master template is required " \
                                    "in the database if no template is " \
                                    "provided on the command line.")
 
    utils.print_info("Using the following IDs:\n" \
                     "    rawfile_id: %d\n" \
                     "    parfile_id: %d\n" \
                     "    template_id: %d" % \
                     (args.rawfile_id, args.parfile_id, args.template_id), 1)
    
    # Load manipulator
    manip = manipulators.load_manipulator(args.manip_name)
    manip.parse_args(leftover_args) 
    # Run pipeline core
    pipeline_core(manip, args.rawfile_id, args.parfile_id, \
                    args.template_id, existdb)


def main():
    # Connect to the database
    db = database.Database()
    db.connect()

    try:
        if args.from_file is not None:
            if args.from_file == '-':
                argfile = sys.stdin
            else:
                if not os.path.exists(args.from_file):
                    raise errors.FileError("The pulsar list (%s) does " \
                                "not appear to exist." % args.from_file)
                argfile = open(args.from_file, 'r')
            numfails = 0
            for line in argfile:
                # Strip comments
                line = line.partition('#')[0].strip()
                if not line:
                    # Skip empty line
                    continue
                try:
                    customargs = copy.deepcopy(args)
                    arglist = line.strip().split()
                    customargs, custom_leftover_args = \
                            parser.parse_known_args(arglist, namespace=customargs)
                    reduce_rawfile(customargs, custom_leftover_args, db)
                except errors.EptaPipelineError:
                    numfails += 1
                    traceback.print_exc()
                    raise
            if args.from_file != '-':
                argfile.close()
            if numfails:
                raise errors.EptaPipelineError(\
                    "\n\n===================================\n" \
                        "The reduction of %d rawfiles failed!\n" \
                        "Please review error output.\n" \
                        "===================================\n" % numfails)
        else:
            reduce_rawfile(args, leftover_args, db)
    finally:
        # Close DB connection
        db.close()


if __name__ == "__main__":
    parser = manipulators.ManipulatorArguments(prog='epta_pipeline', \
                            description='Reduce an already-uploaded ' \
                                'archive. Both a pre-loaded parfile, and a ' \
                                'pre-loaded template must be provided as well. ' \
                                'TOAs generated are loaded into the database, ' \
                                'as is information about the processing run.')
    # Raw data
    rawgroup = parser.add_mutually_exclusive_group(required=False)
    rawgroup.add_argument("--rawfile", dest='rawfile', type=str, \
                        default=None, \
                        help="A raw file to archive/load to DB and " \
                            "generate TOAs for.")
    rawgroup.add_argument('-r', '--rawfile-id', dest='rawfile_id', \
                        type=int, default=None, \
                        help="ID of an already archived/loaded raw data " \
                            "file to use for running the full pipeline.")
    # Ephemeris
    pargroup = parser.add_mutually_exclusive_group(required=False)
    pargroup.add_argument('-p', '--parfile-id', dest='parfile_id', \
                        type=int, default=None, \
                        help="ID of ephemeris to use for running the " \
                            "full pipeline.")
    pargroup.add_argument('--parfile', dest='parfile', type=str, \
                        default=None,
                        help="A parfile to archive/load to DB and " \
                            "use when generating TOAs.")
    # Template profile
    tmpgroup = parser.add_mutually_exclusive_group(required=False)
    tmpgroup.add_argument('-t', '--template-id', dest='template_id',
                        type=int, default=None, \
                        help="ID of template profile to use for running " \
                            "the full pipeline.")
    tmpgroup.add_argument('--template', dest='template', type=str, \
                        default=None,
                        help="A template to archive/load to DB and use " \
                            "when generating TOAs.")
    parser.add_argument('--from-file', dest='from_file', \
                        type=str, default=None, \
                        help="A list of pulsars (one per line) to " \
                            "add. Note: each line can also include " \
                            "alias flags. (Default: load a single " \
                            "pulsar given on the cmd line.)")
    args, leftover_args = parser.parse_known_args()
    if ((args.rawfile is None) or (args.rawfile == '-')) and \
                (args.from_file is None):
        warnings.warn("No input file or --from-file argument given " \
                        "will read from stdin.", \
                        errors.EptaPipelineWarning)
        args.rawfile = None # In case it was set to '-'
        args.from_file = '-'

    main()
