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
                        db.pulsar_aliases.c.alias_name.in_(aliases))
    result = db.execute(select)
    aliases_in_use = []
    for row in result:
        aliases_in_use.append(row['alias_name'])
    result.close()
    if aliases_in_use:
        raise errors.BadInputError("The following proposed aliases are " \
                                    "already in use (each alias must be " \
                                    "unique): '%s'" % \
                                    "', '".join(aliases_in_use))


def add_pulsar(db, pulsar_name, aliases):
    """Add a new pulsar and its aliases to the database.

        Inputs:
            db: A connected Database object.
            pulsar_name: The name of the pulsar.
            aliases: A list of aliases for this pulsar.

        Output:
            pulsar_id: The iD number of the newly inserted pulsar.
    """
    # Add the pulsar's name itself as an alias
    aliases.append(pulsar_name)
    # Make sure no aliases are duplicated in the list
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
                        'alias_name':alias})
    result = db.execute(ins, values)
    result.close()

    db.commit()
    return pulsar_id


def main():
    db = database.Database()
    db.connect()

    pulsar_id = add_pulsar(db, args.pulsar_name, args.aliases)

    print "Successfully inserted new pulsar. " \
                "Returned pulsar_id: %d" % pulsar_id


if __name__=='__main__':
    parser = epu.DefaultArguments(description="Add a new pulsar to the DB")
    parser.add_argument('-p', '--pulsar-name', dest='pulsar_name', \
                        type=str, required=True, \
                        help="The preferred name of the new pulsar. " \
                            "NOTE: This is required.")
    parser.add_argument('-a', '--alias', dest='aliases', \
                        type=str, action='append', default=[], \
                        help="An alias for the pulsar. NOTE: multiple " \
                            "aliases may be provided by including " \
                            "multiple -a/--alias flags.")
    args = parser.parse_args()
    main()
