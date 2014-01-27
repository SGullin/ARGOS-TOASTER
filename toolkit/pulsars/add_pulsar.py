#!/usr/bin/env python
"""Script to add pulsars (and aliases) to the toaster database.
"""
import os.path
import sys
import traceback
import copy
import shlex

from toaster import utils
from toaster import database
from toaster import errors
from toaster.utils import notify
from toaster.utils import cache

SHORTNAME = 'add'
DESCRIPTION = "Add a new pulsar to the DB"


def add_arguments(parser):
    parser.add_argument('pulsar_name', nargs='?', type=str,
                        help="The preferred name of the new pulsar.")
    parser.add_argument('-a', '--alias', dest='aliases',
                        type=str, action='append', default=[],
                        help="An alias for the pulsar. NOTE: multiple "
                             "aliases may be provided by including "
                             "multiple -a/--alias flags.")
    parser.add_argument('--from-file', dest='from_file',
                        type=str, default=None,
                        help="A list of pulsars (one per line) to "
                             "add. Note: each line can also include "
                             "alias flags. (Default: load a single "
                             "pulsar given on the cmd line.)")


def validate_pulsar_name(db, pulsar_name):
    """Check if the given pulsar_name is already in use.
        If so, raise errors.BadInputError.

        Inputs;
            db: A connected Database object.
            pulsar_name: The pulsar name.

        Ouputs:
            None
    """
    select = db.select([db.pulsars], db.pulsars.c.pulsar_name == pulsar_name)
    result = db.execute(select)
    row = result.fetchone()
    result.close()
    if row is not None:
        raise errors.BadInputError("The proposed pulsar name, '%s', "
                                   "is already in use (each pulsar "
                                   "must have a unique name)." %
                                   pulsar_name)


def validate_aliases(aliases, existdb=None):
    """Check if any of the given pulsar aliases are already in use.
        If so, raise errors.BadInputError.

        Inputs:
            aliases: A list of pulsar aliases.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Outputs:
            None
    """
    # Connect to the database
    db = existdb or database.Database()
    db.connect()
    select = db.select([db.pulsar_aliases],
                       db.pulsar_aliases.c.pulsar_alias.in_(aliases))
    result = db.execute(select)
    aliases_in_use = []
    for row in result:
        aliases_in_use.append(row['pulsar_alias'])
    result.close()
    if existdb is None:
        db.close()
    if aliases_in_use:
        raise errors.BadInputError("The following proposed pulsar aliases "
                                   "are already in use (each alias must be "
                                   "unique): '%s'" %
                                   "', '".join(aliases_in_use))


def add_pulsar(pulsar_name, aliases=None, existdb=None):
    """Add a new pulsar and its aliases to the database.

        Inputs:
            pulsar_name: The name of the pulsar.
            aliases: A list of aliases for this pulsar.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            pulsar_id: The ID number of the newly inserted pulsar.
    """
    if aliases is None:
        aliases = []
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    # Add the pulsar's name itself as an alias
    aliases.append(pulsar_name)
    # Make sure no aliases are duplicated in the list
    # TODO: this is susceptible to strings that are different only
    #       by upper/lower characters.
    aliases = list(set(aliases))

    trans = db.begin()  # Open a transaction
    try:
        validate_pulsar_name(db, pulsar_name)
        # Insert new pulsar into the database
        ins = db.pulsars.insert()
        result = db.execute(ins, pulsar_name=pulsar_name)
        pulsar_id = result.inserted_primary_key[0]
        result.close()
        add_pulsar_aliases(pulsar_id, aliases, db)
        # Update the caches
        cache.pulsarname_cache[pulsar_id] = pulsar_name
        cache.pulsaralias_cache[pulsar_id] = aliases
        for alias in aliases:
            cache.pulsarid_cache[alias] = pulsar_id
    except:
        trans.rollback()
        raise
    else:
        trans.commit()
    finally:
        if existdb is None:
            db.close()
    return pulsar_id


def add_pulsar_aliases(pulsar_id, aliases, existdb=None):
    """Add pulsar aliases to DB.

        Inputs:
            pulsar_id: The ID number of the pulsar to add aliases for.
            aliases: A list of aliases for the pulsar.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Outputs:
            None
    """
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    trans = db.begin()
    try:
        validate_aliases(aliases, db)
        # Insert new aliases into the database
        ins = db.pulsar_aliases.insert()
        values = []
        for alias in aliases:
            values.append({'pulsar_id': pulsar_id,
                           'pulsar_alias': alias})
        result = db.execute(ins, values)
        result.close()
    except:
        trans.rollback()
        raise
    else:
        trans.commit()
    finally:
        if existdb is None:
            db.close()


def main(args):
    # Connect to the database
    db = database.Database()
    db.connect()

    try:
        if args.from_file is not None:
            # Re-create parser, so we can read arguments from file
            file_parser = utils.DefaultArguments()
            add_arguments(file_parser)
            if args.pulsar_name is not None:
                raise errors.BadInputError("When adding pulsars from "
                                           "a file, a pulsar name should "
                                           "_not_ be provided on the command "
                                           "line. (The value %s was given on "
                                           "the command line)." %
                                           args.pulsar_name)
            if args.from_file == '-':
                psrlist = sys.stdin
            else:
                if not os.path.exists(args.from_file):
                    raise errors.FileError("The pulsar list (%s) does "
                                           "not appear to exist." %
                                           args.from_file)
                psrlist = open(args.from_file, 'r')
            numfails = 0
            numadded = 0
            for line in psrlist:
                # Strip comments
                line = line.partition('#')[0].strip()
                if not line:
                    # Skip empty line
                    continue
                try:
                    customargs = copy.deepcopy(args)
                    arglist = shlex.split(line.strip())
                    file_parser.parse_args(arglist, namespace=customargs)
                    pulsar_id = add_pulsar(customargs.pulsar_name,
                                           customargs.aliases, db)
                    print "Successfully inserted new pulsar. " \
                        "Returned pulsar_id: %d" % pulsar_id
                    numadded += 1
                except errors.ToasterError:
                    numfails += 1
                    traceback.print_exc()
            if args.from_file != '-':
                psrlist.close()
            if numadded:
                notify.print_success("\n\n===================================\n"
                                     "%d pulsars successfully added\n"
                                     "===================================\n" %
                                     numadded)
            if numfails:
                raise errors.ToasterError(
                    "\n\n===================================\n"
                    "The adding of %d pulsars failed!\n"
                    "Please review error output.\n"
                    "===================================\n" %
                    numfails)
        else:
            pulsar_id = add_pulsar(args.pulsar_name, args.aliases, db)
            print "Successfully inserted new pulsar. " \
                  "Returned pulsar_id: %d" % pulsar_id
    finally:
        # Close DB connection
        db.close()

if __name__ == '__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
