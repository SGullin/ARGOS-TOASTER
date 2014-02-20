#!/usr/bin/env python

from toaster import utils
from toaster import errors
from toaster import database
from toaster.utils import cache
from toaster.utils import notify

SHORTNAME = 'merge'
DESCRIPTION = "Merge a pulsar entry in the database into another entry. " \
              "This is useful when a second entry was created for a " \
              "pulsar rather than creating an alias of an existing entry. " \
              "This is an irreversible process."


def add_arguments(parser):
    parser.add_argument('-p', '--psr', dest='src_psrname', type=str,
                        help="The pulsar to merge. NOTE: All instances "
                             "of this pulsar's corresponding ID number "
                             "will be changed in the database.")
    parser.add_argument('--into', dest='dest_psrname', type=str,
                        help="The pulsar entry to be merged into.")


def merge_pulsar(src_pulsar_id, dest_pulsar_id, existdb=None):
    """Merge one pulsar entry into another.

        Inputs:
            src_pulsar_id: The ID of the pulsar entry that will 
                be merged.
                NOTE: This entry will no longer exist following
                    the merge.
            dest_pulsar_id: The ID of the pulsar entry that will
                be merged into.

            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Outputs:
            None
    """
    notify.print_info("Merging pulsar '%s' (ID: %d) into '%s' (ID: %d)" %
                    (cache.get_pulsarname(src_pulsar_id), src_pulsar_id,
                     cache.get_pulsarname(dest_pulsar_id), dest_pulsar_id), 2)
    # Connect to the database
    db = existdb or database.Database()
    db.connect()
    trans = db.begin()
    try:
        # Update all relevant entries in the database
        tables = [db.pulsar_aliases,
                  db.timfiles,
                  db.rawfiles,
                  db.templates,
                  db.parfiles,
                  db.master_parfiles,
                  db.master_templates,
                  db.toas]
        values = {'pulsar_id': dest_pulsar_id}
        for table in tables:
            update = table.update().\
                           where(table.c.pulsar_id == src_pulsar_id)
            results = db.execute(update, values)
            results.close()

        # Remove now unused entry in the pulsars table
        delete = db.pulsars.delete().\
                    where(db.pulsars.c.pulsar_id == src_pulsar_id)
        results = db.execute(delete)
        results.close()
    except:
        trans.rollback()
        raise
    else:
        trans.commit()
    finally:
        if existdb is None:
            db.close()


def main(args):
    src_pulsar_id = cache.get_pulsarid(args.src_psrname)
    dest_pulsar_id = cache.get_pulsarid(args.dest_psrname)
    if src_pulsar_id == dest_pulsar_id:
        raise errors.BadInputError("Cannot merge '%s' (ID: %d) into "
                                   "itself ('%s')" %
                                   (args.src_psrname, src_pulsar_id,
                                    args.dest_psrname))
    merge_pulsar(src_pulsar_id, dest_pulsar_id)


if __name__ == '__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)