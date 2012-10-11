#!/usr/bin/env python
"""
Script to upload template profiles to the EPTA timing database
"""

import os.path

import config
import database
import errors
import epta_pipeline_utils as epu
import set_master_template as smt
    
def populate_templates_table(db, fn, params, comments):
    # md5sum helper function in epu
    md5 = epu.Get_md5sum(fn);
    path, fn = os.path.split(os.path.abspath(fn))
    
    trans = db.begin()
    # Does this file exist already?
    select = db.select([db.templates.c.template_id, \
                        db.templates.c.pulsar_id]).\
                where(db.templates.c.md5sum==md5)
    results = db.execute(select)
    rows = results.fetchall()
    results.close()
    if len(rows) > 1:
        db.rollback()
        raise errors.InconsistentDatabaseError("There are %d templates " \
                    "with MD5 (%s) in the database already" % (len(rows), md5))
    elif len(rows) == 1:
        psr_id = rows[0]['pulsar_id']
        template_id = rows[0]['template_id']
        if psr_id == params['pulsar_id']:
            warnings.warn("A template with this MD5 (%s) already exists " \
                            "in the DB for this pulsar (ID: %d). " \
                            "The file will not be re-registed into the DB. " \
                            "Doing nothing..." % (md5, psr_id), \
                            errors.EptaPipelineWarning)
            db.commit()
            return template_id
        else:
            db.rollback()
            raise errors.InconsistentDatabaseError("A template with this " \
                            "MD5 (%s) already exists in the DB, but for " \
                            "a different pulsar (ID: %d)!" % (md5, psr_id))
    else:
        # Based on its MD5, this template doesn't already 
        # exist in the DB.

        # Check to see if this pulsar/observing system combination
        # Already has a template
        select = db.select([db.templates.c.template_id]).\
                    where((db.templates.c.pulsar_id==params['pulsar_id']) & \
                          (db.templates.c.obssystem_id==params['obssystem_id']))
        results = db.execute(select)
        rows = results.fetchall()
        results.close()
        if len(rows):
            warnings.warn("This pulsar_id (%d), obssystem_id (%d) " \
                        "combination already has %d templates in the DB. " \
                        "Be sure to correctly set the master template." % \
                        (params['pulsar_id'], params['obssystem_id'], len(rows)))

        # Insert the template
        ins = db.templates.insert()
        values = {'md5sum':md5, \
                  'filename':fn, \
                  'filepath': path, \
                  'user_id':params['user_id'], \
                  'pulsar_id':params['pulsar_id'], \
                  'obssystem_id':params['obssystem_id'], \
                  'nbin':params['nbin'], \
                  'comments':comments}
        result = db.execute(ins, values)
        template_id = result.inserted_primary_key[0]
        result.close()
    db.commit()
    return template_id 


def main():
    fn = args.template
    template_id = load_template(fn)
    print "%s has been loaded to the DB. template_id: %d" % \
            (fn, template_id)


def load_template(fn):
    # Connect to the database
    db = database.Database()
    db.connect()

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
 
        # Register the template into the database
        template_id = populate_templates_table(db, newfn, params, \
                        comments=args.comments)

        mastertemp_id, tempfn = epu.get_master_template(params['pulsar_id'], \
                                                        params['obssystem_id'])
        if mastertemp_id is None:
            # If this is the only template for this pulsar
            # make sure it will be set as the master
            args.is_master = True

        if args.is_master:
            epu.print_info("Setting %s as master template (%s)" % \
                            (newfn, epu.Give_UTC_now()), 1)
            smt.set_as_master_template(db, template_id)
        epu.print_info("Finished with %s - template_id=%d (%s)" % \
                        (fn, template_id, epu.Give_UTC_now()), 1)
    finally:
        # Close DB connection
        db.close()
    return template_id


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
