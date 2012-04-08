#!/usr/bin/env python2.6
"""
Script to promote a previously uploaded template to be a master
"""

#Imported modules
import os.path

import config
import database
import errors
import epta_pipeline_utils as epu


def set_as_master_template(db, template_id):
    query = "REPLACE INTO master_templates " \
                "(template_id, pulsar_id, obsystem_id) " \
            "SELECT template_id, " \
                "pulsar_id, " \
                "obssystem_id " \
            "FROM templates " \
            "WHERE template_id=%d" % template_id
    db.execute(query)


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
    if db.cursor_class=='dict':
        index='template_id'
    else:
        index = 0
    path, fn = os.path.split(os.path.abspath(template))
    md5sum = epu.Get_md5sum(template)
    query = "SELECT template_id " \
            "FROM templates " \
            "WHERE filename=%s " \
                "AND md5sum=%s"
    db.execute(query, (fn, md5sum))
    rows = db.fetchall()
    if rows == 1:
        return rows[0][index]
    elif rows == 0:
        raise errors.EptaPipelineError("No matching template found!")
    else:
        raise errors.InconsistentDatabaseError("Multiple templates have " \
                                                "the same file path/name!")


def main():
    # Connect to the database
    db = database.Database('dict')
    
    try:
        if args.template is not None:
            epu.print_info("Getting template ID for %s using filename and md5" % \
                            args.template, 1)
            # template filename provided. Get template_id
            template_id = get_template_id(db, args.template)
        else:
            template_id = args.template_id
        epu.print_info("Template ID to set as master: %d" % template_id, 1)
        if not args.dry_run:
            set_as_master_template(db, template_id)
    finally:
        # Close DB connection
        db.close()


if __name__=='__main__':
    parser = epu.DefaultArguments(description="Set a standard template " \
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
