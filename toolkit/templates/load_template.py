#!/usr/bin/env python
"""
Script to upload template profiles to the TOASTER database
"""
import copy
import os.path
import warnings
import traceback
import sys
import shlex

import database
import errors
import utils
from toaster.utils import notify
from toaster.utils import datafile
from toolkit.templates import general


SHORTNAME = 'load'
DESCRIPTION = "Upload a standard template " \
              "into the database."


def add_arguments(parser):
    parser.add_argument('--master', dest='is_master',
                        action='store_true', default=False,
                        help="Whether or not the provided file is to be "
                             "set as the master template.")
    parser.add_argument('--comments', dest='comments', type=str,
                        help="Provide comments describing the template.")
    parser.add_argument('--from-file', dest='from_file',
                        type=str, default=None,
                        help="A list of templates (one per line) to "
                             "load. Note: each line can also include "
                             "flags to override what was provided on "
                             "the cmd line for that template. (Default: "
                             "load a single template provided on the "
                             "cmd line.)")
    parser.add_argument('template', nargs='?', type=str,
                        help="File name of the template to upload.")


def populate_templates_table(db, fn, params, comments):
    if comments is None:
        raise errors.BadInputError("A comment is required for every "
                                   "template!")
    # md5sum helper function in utils 
    md5 = datafile.get_md5sum(fn)
    path, fn = os.path.split(os.path.abspath(fn))
    
    trans = db.begin()
    # Does this file exist already?
    select = db.select([db.templates.c.template_id,
                        db.templates.c.pulsar_id]).\
                where(db.templates.c.md5sum == md5)
    results = db.execute(select)
    rows = results.fetchall()
    results.close()
    if len(rows) > 1:
        db.rollback()
        raise errors.InconsistentDatabaseError("There are %d templates "
                                               "with MD5 (%s) in the "
                                               "database already" %
                                               (len(rows), md5))
    elif len(rows) == 1:
        psr_id = rows[0]['pulsar_id']
        template_id = rows[0]['template_id']
        if psr_id == params['pulsar_id']:
            db.commit()
            warnings.warn("A template with this MD5 (%s) already exists "
                          "in the DB for this pulsar (ID: %d). "
                          "The file will not be re-registed into the DB. "
                          "Doing nothing..." % (md5, psr_id),
                          errors.ToasterWarning)
            return template_id
        else:
            db.rollback()
            raise errors.InconsistentDatabaseError("A template with this "
                                                   "MD5 (%s) already exists "
                                                   "in the DB, but for "
                                                   "a different pulsar "
                                                   "(ID: %d)!" %
                                                   (md5, psr_id))
    else:
        # Based on its MD5, this template doesn't already 
        # exist in the DB.

        # Check to see if this pulsar/observing system combination
        # Already has a template
        select = db.select([db.templates.c.template_id]).\
                    where((db.templates.c.pulsar_id == params['pulsar_id']) &
                          (db.templates.c.obssystem_id == params['obssystem_id']))
        results = db.execute(select)
        rows = results.fetchall()
        results.close()
        if len(rows):
            warnings.warn("This pulsar_id (%d), obssystem_id (%d) "
                          "combination already has %d templates in the DB. "
                          "Be sure to correctly set the master template." %
                          (params['pulsar_id'], params['obssystem_id'],
                           len(rows)))

        # Insert the template
        ins = db.templates.insert()
        values = {'md5sum': md5,
                  'filename': fn,
                  'filepath': path,
                  'user_id': params['user_id'],
                  'pulsar_id': params['pulsar_id'],
                  'obssystem_id': params['obssystem_id'],
                  'nbin': params['nbin'],
                  'comments': comments}
        result = db.execute(ins, values)
        template_id = result.inserted_primary_key[0]
        result.close()
    db.commit()
    return template_id 


def load_template(fn, comments, is_master=False, existdb=None):
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    try:
        # Now load the template file into database
        notify.print_info("Working on %s (%s)" % (fn, utils.give_utc_now()), 1)
        
        # Check the template and parse the header
        params = datafile.prep_file(fn)
        
        # Move the file
        destdir = datafile.get_archive_dir(fn, params=params)
        newfn = datafile.archive_file(fn, destdir)
 
        # Register the template into the database
        template_id = populate_templates_table(db, newfn, params,
                                               comments=comments)

        mastertemp_id, tempfn = utils.get_master_template(params['pulsar_id'],
                                                          params['obssystem_id'])
        if mastertemp_id is None:
            # If this is the only template for this pulsar
            # make sure it will be set as the master
            is_master = True

        if is_master:
            notify.print_info("Setting %s as master template (%s)" %
                              (newfn, utils.Give_UTC_now()), 1)
            general.set_as_master_template(template_id, db)
        notify.print_info("Finished with %s - template_id=%d (%s)" %
                          (fn, template_id, utils.Give_UTC_now()), 1)
    finally:
        if not existdb:
            # Close DB connection
            db.close()
    return template_id


def main(args):
    # Allow reading input from stdin
    if ((args.template is None) or (args.template == '-')) and (args.from_file is None):
        warnings.warn("No input file or --from-file argument given "
                      "will read from stdin.", errors.ToasterWarning)
        args.template = None # In case it was set to '-'
        args.from_file = '-'
    
    # Connect to the database
    db = database.Database()
    db.connect()
   
    try:
        if args.from_file is not None:
            # Re-create parser, so we can read arguments from file
            parser = utils.DefaultArguments()
            add_arguments(parser)
            if args.template is not None:
                raise errors.BadInputError("When loading templates from a file, "
                                           "a template value should _not_ be "
                                           "provided on the command line. (The "
                                           "value %s was given on the command "
                                           "line)." % args.template)
            if args.from_file == '-':
                templatelist = sys.stdin
            else:
                if not os.path.exists(args.from_file):
                    raise errors.FileError("The template list (%s) does "
                                           "not appear to exist." % args.from_file)
                templatelist = open(args.from_file, 'r')
            numfails = 0
            numloaded = 0
            for line in templatelist:
                # Strip comments
                line = line.partition('#')[0].strip()
                if not line:
                    # Skip empty line
                    continue
                try:
                    customargs = copy.deepcopy(args)
                    arglist = shlex.split(line.strip())
                    parser.parse_args(arglist, namespace=customargs)
                 
                    fn = customargs.template
                    template_id = load_template(fn, customargs.comments,
                                                customargs.is_master, db)
                    print "%s has been loaded to the DB. template_id: %d" % \
                        (fn, template_id)
                    numloaded += 1
                except errors.ToasterError:
                    numfails += 1
                    traceback.print_exc()
            if args.from_file != '-':
                templatelist.close()
            if numloaded:
                utils.print_success("\n\n===================================\n"
                                    "%d templates successfully loaded\n"
                                    "===================================\n" % numloaded)
            if numfails:
                raise errors.ToasterError(\
                    "\n\n===================================\n"
                    "The loading of %d templates failed!\n"
                    "Please review error output.\n"
                    "===================================\n" % numfails)
        else:
            fn = args.template
            template_id = load_template(fn, args.comments, args.is_master, db)
            print "%s has been loaded to the DB. template_id: %d" % \
                  (fn, template_id)
    finally:
        # Close DB connection
        db.close()


if __name__ == '__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
