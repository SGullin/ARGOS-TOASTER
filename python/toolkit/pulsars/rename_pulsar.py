#!/usr/bin/env python

import utils
import errors
import database

SHORTNAME = 'rename'
DESCRIPTION = "Change the name of a pulsar entry. " \
              "The old name will remain a valid alias."

def add_arguments(parser):
    parser.add_argument('-n', '--name', dest='newname', type=str, \
                        help="The new name of the pulsar entry.")
    parser.add_argument('-p', '--psr', dest='psrname', type=str, \
                        help="The pulsar to rename.")


def check_new_name(pulsar_id, newname):
    """Check if the new name is OK to use for the given 
        pulsar. The new name is invalid if it is already
        in use with a different pulsar_id entry.
        An error is raised if the proposed name is invalid.

        Inputs:
            pulsar_id: The DB ID number of the pulsar to rename.
            newname: The proposed new name.

        Ouputs:
            None
    """
    pulsarid_cache = utils.get_pulsarid_cache()
    if (newname in pulsarid_cache.keys()) and \
            (pulsarid_cache[newname] != pulsar_id):
        used_id = pulsarid_cache[newname]
        raise errors.BadInputError("The proposed pulsar name, '%s', " \
                                    "is already in use with a different " \
                                    "pulsar (%s, ID: %d). Pulsar names and " \
                                    "aliases must refer to a single " \
                                    "pulsar only." % \
                                    (newname, utils.get_pulsarname(used_id), \
                                            used_id))


def rename_pulsar(oldname, newname, existdb=None):
    """Rename pulsar DB entry. An error is raised if the
        renaming in invalid.

        Inputs:
            oldname: The old name of the pulsar entry.
            newname: The proposed new name of the entry.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Ouputs:
            None
    """
    db = existdb or database.Database()
    db.connect()
    # Get the pulsar_id of the entry to rename
    pulsar_id = utils.get_pulsarid(oldname)
    trans = db.begin()
    try:
        # Check if the new name is valid
        check_new_name(pulsar_id, newname)

        # Rename the pulsar entry
        values = {'pulsar_name': newname}
        update = db.pulsars.update().\
                where(db.pulsars.c.pulsar_id == pulsar_id)
        results = db.execute(update, values)
        results.close()

        if newname not in utils.get_pulsarid_cache().keys():
            # Add newname to pulsar_aliases table
            ins = db.pulsar_aliases.insert()
            values = {'pulsar_id':pulsar_id, \
                        'pulsar_alias':newname}
            result = db.execute(ins, values)
            result.close()
    except:
        db.rollback()
        raise
    else:
        db.commit()
    finally:
        if not existdb:
            db.close()


def main(args):
    # Connect to the database
    db = database.Database()
    db.connect()
    
    try:
        if args.newname is None:
            raise errors.BadInputError("A new name must be provided.")
        # Rename the pulsar
        rename_pulsar(args.psrname, args.newname, db)
    finally:
        # Close DB connection
        db.close()


if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
