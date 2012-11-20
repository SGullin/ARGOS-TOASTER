#!/usr/bin/env python
"""
Script to promote a previously uploaded template to be a master
"""

#Imported modules
import os.path

import config

import database
import errors
import utils


def set_as_master_template(db, template_id):
    db.begin()
    # Check if this pulsar/obssystem combiation already has a
    # Master template in the DB
    select = db.select([db.master_templates.c.template_id.label('mtempid'), \
                        db.templates.c.pulsar_id, \
                        db.templates.c.obssystem_id]).\
                where((db.master_templates.c.obssystem_id == \
                                db.templates.c.obssystem_id) & \
                        (db.master_templates.c.pulsar_id == \
                                db.templates.c.pulsar_id) & \
                        (db.templates.c.template_id==template_id))
    result = db.execute(select)
    row = result.fetchone()
    result.close()
    if row:
        if row['mtempid'] == template_id:
            warnings.warn("Template (ID: %d) is already the master " \
                            "template for this pulsar (ID: %d), " \
                            "observing system (ID: %d) combination. " \
                            "Doing nothing..." % (row['mtempid'], \
                            row['pulsar_id'], row['obssystem_id']), \
                            errors.ToasterWarning)
            db.commit()
            return
        else:
            # Update the existing entry
            query = db.master_templates.update().\
                        where((db.master_templates.c.pulsar_id == \
                                    row['pulsar_id']) & \
                              (db.master_templates.c.obssystem_id == \
                                    row['obssystem_id']))
            values = {'template_id':template_id}
    else:
        # Insert a new entry
        query = db.master_templates.insert()
        select = db.select([db.templates.c.pulsar_id, \
                            db.templates.c.obssystem_id]).\
                    where(db.templates.c.template_id==template_id)
        result = db.execute(select)
        row = result.fetchone()
        result.close()

        values = {'template_id':template_id, \
                  'pulsar_id':row['pulsar_id'], \
                  'obssystem_id':row['obssystem_id']}
    try:
        result = db.execute(query, values)
    except:
        db.rollback()
        raise
    else:
        db.commit()
        result.close()


def get_template_id(db, template):
    """Given a template file path find its template_id number.
        
        Inputs:
            db: connected database.Database object.
            template: the path to a template file.

        Output:
            template_id: the corresponding template_id value.
    """
    # Make sure we can read the result regardless which cursor class
    # is being used
    path, fn = os.path.split(os.path.abspath(template))
    md5sum = utils.Get_md5sum(template)
    select = db.select([db.templates.c.template_id, \
                        db.templates.c.md5sum]).\
                where(db.templates.c.filename==fn)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if rows == 1:
        row = rows[0]
        if row['md5sum'] != md5sum:
            raise errors.FileError("Template (%s) found in database but " \
                                    "MD5sum in DB (%s) doesn't match MD5sum" \
                                    "of the file provided (%s)!" % \
                                    (filename, row[0].md5sum, md5sum))
        else:
            return row['template_id']
    elif rows == 0:
        raise errors.ToasterError("No matching template found! " \
                                        "Use 'load_template.py' to add " \
                                        "a new template to the DB.")
    else:
        raise errors.InconsistentDatabaseError("Multiple templates have " \
                                                "the same file path/name!")


def main():
    # Connect to the database
    db = database.Database()
    db.connect()

    try:
        if args.template is not None:
            utils.print_info("Getting template ID for %s using filename and md5" % \
                            args.template, 1)
            # template filename provided. Get template_id
            template_id = get_template_id(db, args.template)
        else:
            template_id = args.template_id
        utils.print_info("Template ID to set as master: %d" % template_id, 1)
        if not args.dry_run:
            set_as_master_template(db, template_id)
    finally:
        # Close DB connection
        db.close()


if __name__=='__main__':
    parser = utils.DefaultArguments(description="Set a standard template " \
                                              "already uploaded into the " \
                                              "database to be a master.")
    parser.add_argument('-n', '--dry-run', dest='dry_run', \
                        action='store_true', \
                        help="Do not modify database.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--template-id', dest='template_id', type=int, \
                        help="template_id value of template to set as master.")
    group.add_argument('--template', dest='template', type=int, \
                        help="Template file to set as master.")
    args = parser.parse_args()
    main()
