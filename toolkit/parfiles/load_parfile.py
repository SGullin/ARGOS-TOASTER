#!/usr/bin/env python

"""
Script to upload parfiles to the TOASTER database.
"""
import copy
import os.path
import warnings
import types
import traceback
import sys
import shlex

from toaster import config
from toaster import database
from toaster import errors
from toaster.toolkit.parfiles import general
from toaster import utils
from toaster.utils import datafile
from toaster.utils import notify


SHORTNAME = 'load'
DESCRIPTION = "Upload a parfile into the database."


def add_arguments(parser):
    parser.add_argument('--master', dest='is_master',
                        action='store_true', default=False,
                        help="Whether or not the provided file is to be "
                             "set as the master parfile.")
    #parser.add_argument( '--comments', dest='comments', required=True,
    #                     type = str,
    #                     help='Provide comments describing the par files.')
    parser.add_argument('--from-file', dest='from_file',
                        type=str, default=None,
                        help="A list of parfiles (one per line) to "
                             "load. Note: each line can also include "
                             "flags to override what was provided on "
                             "the cmd line for that parfile. (Default: "
                             "load a single parfile provided on the "
                             "cmd line.)")
    parser.add_argument('parfile', nargs='?', type=str,
                        help="Parameter file to upload.")


def populate_parfiles_table(db, fn, params):
    # md5sum helper function in utils 
    md5 = datafile.get_md5sum(fn)
    path, fn = os.path.split(os.path.abspath(fn))
   
    db.begin()  # Begin a transaction
    # Does this file exist already?
    select = db.select([db.parfiles.c.parfile_id, db.parfiles.c.pulsar_id],
                        db.parfiles.c.md5sum == md5)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if len(rows) > 1:
        db.rollback()
        raise errors.InconsistentDatabaseError("There are %d parfiles "
                                               "with MD5 (%s) in the "
                                               "database already" %
                                               (len(rows), md5))
    elif len(rows) == 1:
        parfile_id, psr_id = rows[0]
        if psr_id == params['pulsar_id']:
            warnings.warn("A parfile with this MD5 (%s) already exists "
                          "in the DB for this pulsar (ID: %d). "
                          "The file will not be re-registed into the DB. "
                          "Doing nothing..." % (md5, psr_id),
                          errors.ToasterWarning)
        else:
            db.rollback()
            raise errors.InconsistentDatabaseError("A parfile with this "
                                                   "MD5 (%s) already exists "
                                                   "in the DB, but for "
                                                   "a different pulsar "
                                                   "(ID: %d)!" % (md5, psr_id))
    else:
        # Based on its MD5, this parfile doesn't already 
        # exist in the DB. Insert it.

        # Insert the parfile
        ins = db.parfiles.insert()
        values = {'md5sum': md5,
                  'filename': fn,
                  'filepath': path}

        values.update(params)
        result = db.execute(ins, values)
        parfile_id = result.inserted_primary_key[0]
        result.close()
    
    db.commit()
    return parfile_id 


def load_parfile(fn, is_master=False, existdb=None):
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    try:
        # Now load the parfile file into database
        notify.print_info("Working on %s (%s)" % (fn, utils.give_utc_now()), 1)
        
        # Check the parfile and parse it
        params = general.prep_parfile(fn)

        # Archive the parfile
        destdir = os.path.join(config.cfg.data_archive_location,
                               'parfiles', params['name'])
        newfn = datafile.archive_file(fn, destdir)

        # Register the parfile into the database
        parfile_id = populate_parfiles_table(db, newfn, params)
       
        masterpar_id, parfn = general.get_master_parfile(params['pulsar_id'])
        if masterpar_id is None:
            # If this is the only parfile for this pulsar 
            # make sure it will be set as the master
            is_master = True

        if is_master:
            notify.print_info("Setting %s as master parfile (%s)" %
                              (newfn, utils.give_utc_now()), 1)
            general.set_as_master_parfile(parfile_id, db)
        notify.print_info("Finished with %s - parfile_id=%d (%s)" %
                          (fn, parfile_id, utils.give_utc_now()), 1)
    finally:
        if not existdb:
            # Close DB connection
            db.close()
    return parfile_id


def main(args):
    # Allow reading arguments from stdin
    if ((args.parfile is None) or (args.parfile == '-')) and \
                (args.from_file is None):
        warnings.warn("No input file or --from-file argument given "
                      "will read from stdin.",
                      errors.ToasterWarning)
        args.parfile = None # In case it was set to '-'
        args.from_file = '-'
    
    # Connect to the database
    db = database.Database()
    db.connect()
   
    try:
        if args.from_file is not None:
            # Re-create parser, so we can read arguments from file
            parser = utils.DefaultArguments()
            add_arguments(parser)
            if args.parfile is not None:
                raise errors.BadInputError("When loading parfiles from "
                                           "a file, a parfile value should "
                                           "_not_ be provided on the command "
                                           "line. (The value %s was given on "
                                           "the command line)." %
                                           args.parfile)
            if args.from_file == '-':
                parlist = sys.stdin
            else:
                if not os.path.exists(args.from_file):
                    raise errors.FileError("The parfile list (%s) does "
                                           "not appear to exist." %
                                           args.from_file)
                parlist = open(args.from_file, 'r')
            numfails = 0
            numloaded = 0
            for line in parlist:
                # Strip comments
                line = line.partition('#')[0].strip()
                if not line:
                    # Skip empty line
                    continue
                try:
                    customargs = copy.deepcopy(args)
                    arglist = shlex.split(line.strip())
                    parser.parse_args(arglist, namespace=customargs)
                 
                    fn = customargs.parfile
                    parfile_id = load_parfile(customargs.parfile,
                                              customargs.is_master, db)
                    print("%s has been loaded to the DB. parfile_id: %d" % \
                        (fn, parfile_id))
                    numloaded += 1
                except errors.ToasterError:
                    numfails += 1
                    traceback.print_exc()
            if args.from_file != '-':
                parlist.close()
            if numloaded:
                utils.print_success("\n\n===================================\n"
                                    "%d parfiles successfully loaded\n"
                                    "===================================\n" %
                                    numloaded)
            if numfails:
                raise errors.ToasterError(
                    "\n\n===================================\n"
                    "The loading of %d parfiles failed!\n"
                    "Please review error output.\n"
                    "===================================\n" % numfails)
        else:
            fn = args.parfile
            parfile_id = load_parfile(fn)
            print("%s has been loaded to the DB. parfile_id: %d" % \
                (fn, parfile_id))
    finally:
        # Close DB connection
        db.close()


if __name__ == '__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)