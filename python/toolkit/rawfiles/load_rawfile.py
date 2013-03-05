#!/usr/bin/env python
"""A script to load information about data archives into the database.
"""
import sys
import os.path
import warnings
import traceback
import copy
import shlex

import config
import utils
import errors
import database
import diagnostics
import diagnose_rawfile

SHORTNAME = 'load'
DESCRIPTION = "Archive a single raw file, " \
              "and load its info into the database."


def add_arguments(parser):
    parser.add_argument('--from-file', dest='from_file', \
                        type=str, default=None, \
                        help="A list of rawfiles (one per line) to " \
                            "load. (Default: load a raw file provided " \
                            "on the cmd line.)")
    parser.add_argument("rawfile", nargs='?', type=str, \
                        help="File name of the raw file to upload.")


def populate_rawfiles_table(db, archivefn, params):
    # md5sum helper function in utils 
    md5 = utils.Get_md5sum(archivefn)
    path, fn = os.path.split(os.path.abspath(archivefn))
    size = os.path.getsize(archivefn) # File size in bytes

    trans = db.begin()
    # Does this file exist already?
    select = db.select([db.rawfiles.c.rawfile_id, \
                        db.rawfiles.c.pulsar_id]).\
                where(db.rawfiles.c.md5sum==md5)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if len(rows) > 1:
        trans.rollback()
        raise errors.InconsistentDatabaseError("There are %d rawfiles " \
                    "with MD5 (%s) in the database already" % (len(rows), md5))
    elif len(rows) == 1:
        rawfile_id = rows[0]['rawfile_id']
        psr_id = rows[0]['pulsar_id']
        if psr_id == params['pulsar_id']:
            warnings.warn("A rawfile with this MD5 (%s) already exists " \
                            "in the DB for this pulsar (ID: %d). " \
                            "The file will not be re-registed into the DB. " \
                            "Doing nothing..." % (md5, psr_id), \
                            errors.ToasterWarning)
            trans.commit()
            return rawfile_id
        else:
            trans.rollback()
            raise errors.InconsistentDatabaseError("A rawfile with this " \
                            "MD5 (%s) already exists in the DB, but for " \
                            "a different pulsar (ID: %d)!" % (md5, psr_id))
    else:
        utils.print_info("Inserting rawfile (%s) into DB." % fn, 3)
        # Based on its MD5, this rawfile doesn't already 
        # exist in the DB. Insert it.

        # Insert the file
        ins = db.rawfiles.insert()
        values = {'md5sum':md5, \
                  'filename':fn, \
                  'filepath':path, \
                  'filesize':size, \
                  'coord':'%s,%s' % (params['ra'],params['dec'])}
        values.update(params)
        result = db.execute(ins, values)
        rawfile_id = result.inserted_primary_key[0]
        result.close()

        # Create rawfile diagnostics
        diags = []
        for diagname in config.cfg.default_rawfile_diagnostics:
            diagcls = diagnostics.get_diagnostic_class(diagname)
            try:
                diags.append(diagcls(archivefn))
            except errors.DiagnosticNotApplicable, e:
                utils.print_info("Diagnostic isn't applicable: %s. " \
                                "Skipping..." % str(e), 1)
        if diags:
            # Load processing diagnostics
            diagnose_rawfile.insert_rawfile_diagnostics(rawfile_id, diags, \
                                                        existdb=db)
    trans.commit()
    return rawfile_id


def load_rawfile(fn, existdb=None):
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    try:
        # Enter information in rawfiles table
        utils.print_info("Working on %s (%s)" % (fn, utils.Give_UTC_now()), 1)
        # Check the file and parse the header
        params = utils.prep_file(fn)
        
        # Move the File
        destdir = utils.get_archive_dir(fn, params=params)
        newfn = utils.archive_file(fn, destdir)
        
        utils.print_info("%s moved to %s (%s)" % (fn, newfn, utils.Give_UTC_now()), 1)

        # Register the file into the database
        rawfile_id = populate_rawfiles_table(db, newfn, params)
        
        utils.print_info("Successfully loaded %s - rawfile_id=%d (%s)" % \
                (fn, rawfile_id, utils.Give_UTC_now()), 1)
    finally:
        if not existdb:
            # Close DB connection
            db.close()
    return rawfile_id
    

def main(args):
    # Allow arguments to be read from stdin
    if ((args.rawfile is None) or (args.rawfile == '-')) and \
                (args.from_file is None):
        warnings.warn("No input file or --from-file argument given " \
                        "will read from stdin.", \
                        errors.ToasterWarning)
        args.rawfile = None # In case it was set to '-'
        args.from_file = '-'
    # Connect to the database
    db = database.Database()
    db.connect()
   
    try:
        if args.from_file is not None:
            # Re-create parser, so we can read arguments from file
            parser = utils.DefaultArguments()
            add_arguments(parser)
            if args.rawfile is not None:
                raise errors.BadInputError("When loading rawfiles from " \
                                "a file, a rawfile value should _not_ be " \
                                "provided on the command line. (The value " \
                                "%s was given on the command line)." % \
                                args.rawfile)
            if args.from_file == '-':
                rawlist = sys.stdin
            else:
                if not os.path.exists(args.from_file):
                    raise errors.FileError("The rawfile list (%s) does " \
                                "not appear to exist." % args.from_file)
                rawlist = open(args.from_file, 'r')
            numfails = 0
            numloaded = 0
            for line in rawlist:
                # Strip comments
                line = line.partition('#')[0].strip()
                if not line:
                    # Skip empty line
                    continue
                try:
                    # parsing arguments is overkill at the moment 
                    # since 'load_rawfile.py' doesn't take any 
                    # arguments, but this makes the code more future-proof
                    customargs = copy.deepcopy(args)
                    arglist = shlex.split(line.strip())
                    parser.parse_args(arglist, namespace=customargs)
                 
                    fn = customargs.rawfile
                    rawfile_id = load_rawfile(fn, db)
                    print "%s has been loaded to the DB. rawfile_id: %d" % \
                                (fn, rawfile_id)
                    numloaded += 1
                except errors.ToasterError:
                    numfails += 1
                    traceback.print_exc()
            if args.from_file != '-':
                rawlist.close()
            if numloaded:
                utils.print_success("\n\n===================================\n" \
                                    "%d rawfiles successfully loaded\n" \
                                    "===================================\n" % numloaded)
            if numfails:
                raise errors.ToasterError(\
                    "\n\n===================================\n" \
                        "The loading of %d rawfiles failed!\n" \
                        "Please review error output.\n" \
                        "===================================\n" % numfails)
        else:
            fn = args.rawfile
            rawfile_id = load_rawfile(fn, db)
            print "%s has been loaded to the DB. rawfile_id: %d" % \
                (fn, rawfile_id)
    finally:
        # Close DB connection
        db.close()


if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)

