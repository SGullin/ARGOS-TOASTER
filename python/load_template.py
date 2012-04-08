#!/usr/bin/env python2.6
"""
Script to upload template profiles to the EPTA timing database
"""

#Imported modules
import os.path

import config
import database
import errors
import epta_pipeline_utils as epu
import set_master_template as smt
    
def populate_templates_table(db, fn, params, comments, is_analytic):
    # md5sum helper function in epu
    md5 = epu.Get_md5sum(fn);
    path, fn = os.path.split(os.path.abspath(fn))
    
    # Does this file exist already?
    query = "SELECT template_id FROM templates WHERE md5sum = '%s'" % md5
    db.execute(query)
    rows = db.fetchall()
    if rows:
        raise errors.DatabaseError("A template with MD5 (%s) in " \
                                    "database already" % md5)
    
    # Insert the file
    query = "INSERT INTO templates " + \
            "SET md5sum = '%s', " % md5 + \
                "filename = '%s', " % fn + \
                "filepath = '%s', " % path + \
                "user_id = '%s', " % params['user_id'] + \
                "add_time = NOW(), " + \
                "pulsar_id = '%s', " % params['pulsar_id'] + \
                "obssystem_id = '%s', " % params['obssystem_id'] + \
                "nbin = %d, " % params['nbin'] + \
                "is_analytic = %d, " % is_analytic + \
                "comments = '%s' " % comments
    db.execute(query)

    # Get the template_id of the file that was just entered
    query = "SELECT LAST_INSERT_ID()"
    template_id = db.execute_and_fetchone(query)[0]
    return template_id 


def main():
    fn = args.template
    
    # Connect to the database
    db = database.Database()
    
    try:
        # Now load the template file into database
        epu.print_info("Working on %s (%s)" % (fn, epu.Give_UTC_now()), 1)
        
        # Check the template and parse the header
        params = epu.prep_file(fn)
        
        # Move the file
        destdir = epu.get_archive_dir(fn, site=params['telescop'], \
                    backend=params['backend'], receiver=params['rcvr'], \
                    psrname=params['name'])
        newfn = epu.archive_file(fn, destdir)
 
        epu.print_info("%s move to %s (%s)" % (fn, newfn, epu.Give_UTC_now()), 1)
 
        # Register the template into the database
        template_id = populate_templates_table(db, newfn, params, \
                        comments=args.comments, is_analytic=False)
        if args.is_master:
            epu.print_info("Setting %s as master template (%s)" % \
                            (newfn, epu.Give_UTC_now()), 1)
            smt.set_as_master_template(db, template_id)
        epu.print_info("Finished with %s - template_id=%d (%s)" % \
                        (fn, template_id, epu.Give_UTC_now()), 1)
    finally:
        # Close DB connection
        db.close()


if __name__=='__main__':
    parser = epu.DefaultArguments(description="Upload a standard template " \
                                              "into the database.")
    parser.add_argument('--master', dest='is_master', \
                         action = 'store_true', default = False, \
                         help = "Whether or not the provided file is to be " \
                                "set as the master template.")
    parser.add_argument('--comments', dest='comments', required=True, type=str, \
                        help="Provide comments describing the template.")
    parser.add_argument('template', type=str, \
                        help="File name of the template to upload.")
    args = parser.parse_args()
    main()
