#!/usr/bin/env python
"""
move_template.py

Move a template and update the database accordingly.

Patrick Lazarus, Mar 10, 2014
"""

import os
import shutil

from toaster import utils
from toaster.toolkit.templates import general
from toaster.utils import notify
from toaster import database
from toaster import errors

SHORTNAME = 'move'
DESCRIPTION = 'Move a template and update the database accordingly.'


def add_arguments(parser):
    parser.add_argument('-t', '--template-id', dest='template_id',
                        type=int, required=True,
                        help="ID of ephemeris to move.")
    parser.add_argument("--dest", dest='dest', type=str,
                        help="Where template will be moved to.")


def update_template_entry(template_id, dest, existdb=None):
    """Update the database to reflect a moved template.

        Input:
            template_id: The ID number of the template to remove.
            dest: The new destination of the template.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Outputs:
            None
    """
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    # Remove template from DB
    values = {'filepath': os.path.dirname(dest),
              'filename': os.path.basename(dest)}
    update = db.templates.update().\
                where(db.templates.c.template_id == template_id)
    result = db.execute(update, values)
    result.close()
    if not existdb:
        # Close DB connection
        db.close()


def main(args):
    move_template(args.template_id, args.dest)


def move_template(template_id, dest, existdb=None):
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    trans = db.begin()
    try:
        template = general.get_template_from_id(template_id, existdb=db)

        dest = os.path.abspath(dest)
        if os.path.isdir(dest):
            dest = os.path.join(dest, os.path.basename(template))
        elif os.path.isfile(dest):
            raise errors.FileError("Template destination file (%s) "
                                   "already exists!" % dest)
        # Deal with the template itself
        notify.print_info("Moving template %s to %s" % (template, dest))

        shutil.copyfile(template, dest)
        # Now remove the template entry from the DB
        update_template_entry(template_id, dest)
    except:
        # Failure
        trans.rollback()
        raise
    else:
        # Success
        os.remove(template)
        trans.commit()
    finally:
        db.close()


if __name__ == "__main__":
    parser = utils.DefaultArguments(prog='move_template.py',
                                    description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)