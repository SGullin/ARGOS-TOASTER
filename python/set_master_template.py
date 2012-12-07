#!/usr/bin/env python
"""
Script to promote a previously uploaded template to be a master
"""

#Imported modules
import os.path

import database
import errors
import utils


def main():
    # Connect to the database
    db = database.Database()
    db.connect()

    try:
        if args.template is not None:
            utils.print_info("Getting template ID for %s using filename and md5" % \
                            args.template, 1)
            # template filename provided. Get template_id
            template_id = utils.get_template_id(args.template, db)
        else:
            template_id = args.template_id
        utils.print_info("Template ID to set as master: %d" % template_id, 1)
        if not args.dry_run:
            utils.set_as_master_template(template_id, db)
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
