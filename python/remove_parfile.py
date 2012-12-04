#!/usr/bin/env python
"""
remove_parfile.py

Remove a parfile from the database.
NOTE: Only parfiles that have not yet been used for processing 
may be removed.

Patrick Lazarus, Dec 2, 2012
"""

import utils
import database
import errors


def remove_parfile_entry(parfile_id, existdb=None):
    """Remove parfile entry from the database if it has 
        not yet been used for any processing.

        Input:
            parfile_id: The ID number of the parfile to remove.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Outputs:
            None
    """
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    # Check if parfile has been used for any processing
    select = db.select([db.process.c.process_id]).\
                where(db.process.c.parfile_id == parfile_id)
    result = db.execute(select)
    row = result.fetchone()
    result.close()
    if row is not None:
        if not existdb:
            # Close DB connection
            db.close()
        raise errors.ToasterError("Cannot remove parfile (ID: %d). " \
                "It has been been used for processing." % parfile_id)
    else:
        # Remove parfile from DB
        delete = db.master_parfiles.delete().\
                    where(db.master_parfiles.c.parfile_id == parfile_id)
        result = db.execute(delete)
        result.close()
        delete = db.parfiles.delete().\
                    where(db.parfiles.c.parfile_id == parfile_id)
        result = db.execute(delete)
        result.close()
        if not existdb:
            # Close DB connection
            db.close()


def main():
    # Establish a DB connection
    db = database.Database()
    db.connect()

    trans = db.begin()
    try:
        parfile = utils.get_parfile_from_id(args.parfile_id, existdb=db)
        # First remove the parfile entry from the DB
        remove_parfile_entry(args.parfile_id)
        # Now deal with the parfile itself
        if args.action=='leave':
            # Do nothing
            pass
        elif args.action=='move':
            if not args.dest:
                raise errors.BadInputError("Destination must be provided " \
                        "when moving parfile.")
            utils.print_info("Moving parfile %s to %s" % (parfile, args.dest))
            shutil.move(parfile, args.dest)
        elif args.action=='delete':
            utils.print_info("Deleting parfile %s" % parfile)
            os.remove(parfile)
        else:
            raise UnrecognizedValueError("The action provided (%s) is not " \
                                        "recognized." % args.action)
    except:
        trans.rollback()
        raise
    else:
        trans.commit()
    finally:
        db.close()



if __name__ == "__main__":
    parser = utils.DefaultArguments(prog='remove_parfile.py', \
                            description='Remove a parfile from the database. ' \
                                'NOTE: Only parfiles that have not yet been ' \
                                'used for processing may be removed.')
    parser.add_argument('-p', '--parfile-id', dest='parfile_id', \
                        type=int, required=True, \
                        help="ID of ephemeris to remove.")
    actiongroup = parser.add_mutually_exclusive_group(required=False)
    actiongroup.add_argument("--move-to", dest='action', action='store_const', \
                        const='move', default='leave', \
                        help="Move the parfile to after removal " \
                            "from the database. The '--dest' argument " \
                            "providing the destination, is required. " \
                            "(Default: Leave the parfile in the archive.)")
    actiongroup.add_argument("--delete", dest='action', action='store_const', \
                        const='delete', default='leave', \
                        help="Delete the parfile after removal from the " \
                            "database. (Default: Leave the parfile in the " \
                            "archive.)")
    actiongroup.add_argument("--leave", dest='action', action='store_const', \
                        const='leave', default='leave', \
                        help="Leave the parfile in the archive after " \
                            "removal from the database. (Default: This " \
                            "is the default.)")
    parser.add_argument("--dest", dest='dest', type=str, \
                        help="Where parfile will be moved to. NOTE: " \
                            "this arg only gets used if '--move-to' is " \
                            "being used.")
    args = parser.parse_args()
    main()
