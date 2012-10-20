#!/usr/bin/env python
"""Script to add pulsars (and aliases) to the toaster database.
"""
import os.path
import sys
import traceback
import copy

import epta_pipeline_utils as epu
import database
import errors


def validate_pulsar_name(db, pulsar_name):
    """Check if the given pulsar_name is already in use.
        If so, raise errors.BadInputError.

        Inputs;
            db: A connected Database object.
            pulsar_name: The pulsar name.

        Ouputs:
            None
    """
    select = db.select([db.pulsars], db.pulsars.c.pulsar_name==pulsar_name)
    result = db.execute(select)
    row = result.fetchone()
    result.close()
    if row is not None:
        raise errors.BadInputError("The proposed pulsar name, '%s', " \
                                    "is already in use (each pulsar " \
                                    "must have a unique name)." % \
                                    pulsar_name)


def validate_aliases(db, aliases):
    """Check if any of the given pulsar aliases are already in use.
        If so, raise errors.BadInputError.

        Inputs:
            db: A connected Database object.
            aliases: A list of pulsar aliases.

        Outputs:
            None
    """
    select = db.select([db.pulsar_aliases], \
                        db.pulsar_aliases.c.pulsar_alias.in_(aliases))
    result = db.execute(select)
    aliases_in_use = []
    for row in result:
        aliases_in_use.append(row['pulsar_alias'])
    result.close()
    if aliases_in_use:
        raise errors.BadInputError("The following proposed pulsar aliases " \
                                    "are already in use (each alias must be " \
                                    "unique): '%s'" % \
                                    "', '".join(aliases_in_use))


def add_pulsar(db, pulsar_name, aliases=[]):
    """Add a new pulsar and its aliases to the database.

        Inputs:
            db: A connected Database object.
            pulsar_name: The name of the pulsar.
            aliases: A list of aliases for this pulsar.

        Output:
            pulsar_id: The ID number of the newly inserted pulsar.
    """
    # Add the pulsar's name itself as an alias
    aliases.append(pulsar_name)
    # Make sure no aliases are duplicated in the list
    # TODO: this is suceptible to strings that are different only
    #       by upper/lower characters.
    aliases = list(set(aliases))

    trans = db.begin() # Open a transaction
    try:
        validate_pulsar_name(db, pulsar_name)
        validate_aliases(db, aliases)
    except errors.BadInputError:
        db.rollback()
        raise
    # Insert new pulsar into the database
    ins = db.pulsars.insert()
    result = db.execute(ins, pulsar_name=pulsar_name)
    pulsar_id = result.inserted_primary_key[0]
    result.close()

    # Insert new aliases into the database
    ins = db.pulsar_aliases.insert()
    values = []
    for alias in aliases:
        values.append({'pulsar_id':pulsar_id, \
                        'pulsar_alias':alias})
    result = db.execute(ins, values)
    result.close()

    db.commit()
    return pulsar_id


def main():
    # Connect to the database
    db = database.Database()
    db.connect()

    try:
        if args.from_file is not None:
            if args.pulsar_name is not None:
                raise errors.BadInputError("When adding pulsars from " \
                                "a file, a pulsar name should _not_ be " \
                                "provided on the command line. (The value " \
                                "%s was given on the command line)." % \
                                args.pulsar_name)
            if args.from_file == '-':
                psrlist = sys.stdin
            else:
                if not os.path.exists(args.from_file):
                    raise errors.FileError("The pulsar list (%s) does " \
                                "not appear to exist." % args.from_file)
                psrlist = open(args.from_file, 'r')
            numfails = 0
            for line in psrlist:
                # Strip comments
                line = line.partition('#')[0].strip()
                if not line:
                    # Skip empty line
                    continue
                try:
                    customargs = copy.deepcopy(args)
                    arglist = line.strip().split()
                    parser.parse_args(arglist, namespace=customargs)
                    pulsar_id = add_pulsar(db, customargs.pulsar_name, \
                                            customargs.aliases)
                    print "Successfully inserted new pulsar. " \
                        "Returned pulsar_id: %d" % pulsar_id
                except errors.EptaPipelineError:
                    numfails += 1
                    traceback.print_exc()
            if args.from_file != '-':
                psrlist.close()
            if numfails:
                raise errors.EptaPipelineError(\
                    "\n\n===================================\n" \
                        "The adding of %d pulsars failed!\n" \
                        "Please review error output.\n" \
                        "===================================\n" % numfails)
        else:
            pulsar_id = add_pulsar(db, args.pulsar_name, args.aliases)
            print "Successfully inserted new pulsar. " \
                        "Returned pulsar_id: %d" % pulsar_id
    finally:
        # Close DB connection
        db.close()

if __name__=='__main__':
    parser = epu.DefaultArguments(description="Add a new pulsar to the DB")
    parser.add_argument('pulsar_name', nargs='?', type=str, \
                        help="The preferred name of the new pulsar.")
    parser.add_argument('-a', '--alias', dest='aliases', \
                        type=str, action='append', default=[], \
                        help="An alias for the pulsar. NOTE: multiple " \
                            "aliases may be provided by including " \
                            "multiple -a/--alias flags.")
    parser.add_argument('--from-file', dest='from_file', \
                        type=str, default=None, \
                        help="A list of pulsars (one per line) to " \
                            "add. Note: each line can also include " \
                            "alias flags. (Default: load a single " \
                            "pulsar given on the cmd line.)")
    args = parser.parse_args()
    main()
