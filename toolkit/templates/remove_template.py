#!/usr/bin/env python
"""
remove_template.py

Remove a template from the database.
NOTE: Only templates that have not yet been used for processing 
may be removed.

Patrick Lazarus, Dec 4, 2012
"""

import os
import shutil

from toaster import utils
from toaster.toolkit.templates import general
from toaster.utils import notify
from toaster import database
from toaster import errors

SHORTNAME = 'remove'
DESCRIPTION = 'Remove a template from the database. ' \
                'NOTE: Only templates that have not yet been ' \
                'used for processing may be removed.'


def add_arguments(parser):
    parser.add_argument('-t', '--template-id', dest='template_id',
                        type=int, required=True,
                        help="ID of ephemeris to remove.")
    actiongroup = parser.add_mutually_exclusive_group(required=False)
    actiongroup.add_argument("--move-to", dest='action', action='store_const',
                        const='move', default='leave',
                        help="Move the template to after removal "
                             "from the database. The '--dest' argument "
                             "providing the destination, is required. "
                             "(Default: Leave the template in the archive.)")
    actiongroup.add_argument("--delete", dest='action', action='store_const',
                        const='delete', default='leave',
                        help="Delete the template after removal from the "
                             "database. (Default: Leave the template in the "
                             "archive.)")
    actiongroup.add_argument("--leave", dest='action', action='store_const',
                        const='leave', default='leave',
                        help="Leave the template in the archive after "
                             "removal from the database. (Default: This "
                             "is the default.)")
    parser.add_argument("--dest", dest='dest', type=str,
                        help="Where template will be moved to. NOTE: "
                             "this arg only gets used if '--move-to' is "
                             "being used.")


def remove_template_entry(template_id, existdb=None):
    """Remove template entry from the database if it has 
        not yet been used for any processing.

        Input:
            template_id: The ID number of the template to remove.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Outputs:
            None
    """
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    # Check if template has been used for any processing
    select = db.select([db.process.c.process_id]).\
                where(db.process.c.template_id == template_id)
    result = db.execute(select)
    row = result.fetchone()
    result.close()
    if row is not None:
        if not existdb:
            # Close DB connection
            db.close()
        raise errors.ToasterError("Cannot remove template (ID: %d). "
                                  "It has been been used for processing." %
                                  template_id)
    else:
        # Remove template from DB
        delete = db.master_templates.delete().\
                    where(db.master_templates.c.template_id == template_id)
        result = db.execute(delete)
        result.close()
        delete = db.templates.delete().\
                    where(db.templates.c.template_id == template_id)
        result = db.execute(delete)
        result.close()
        if not existdb:
            # Close DB connection
            db.close()


def main(args):
    # Establish a DB connection
    db = database.Database()
    db.connect()

    trans = db.begin()
    try:
        template = general.get_template_from_id(args.template_id, existdb=db)
        # First remove the template entry from the DB
        remove_template_entry(args.template_id)
        # Now deal with the template itself
        if args.action == 'leave':
            # Do nothing
            pass
        elif args.action == 'move':
            if not args.dest:
                raise errors.BadInputError("Destination must be provided "
                                           "when moving template.")
            notify.print_info("Moving template %s to %s" % (template, args.dest))
            shutil.move(template, args.dest)
        elif args.action == 'delete':
            notify.print_info("Deleting template %s" % template)
            os.remove(template)
        else:
            raise errors.UnrecognizedValueError("The action provided (%s) "
                                                "is not recognized." %
                                                args.action)
    except:
        trans.rollback()
        raise
    else:
        trans.commit()
    finally:
        db.close()


if __name__ == "__main__":
    parser = utils.DefaultArguments(prog='remove_template.py',
                                    description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
