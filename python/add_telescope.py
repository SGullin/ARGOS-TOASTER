#!/usr/bin/env python
import utils
import database
import errors


def validate_telescope(db, name, abbrev, code):
    """Check if the given telescope name, abbreviation, or code
        is already in use. If so, raise errors.BadInputError.

        Inputs:
            db: A connected Database object.
            name: The name of the telescope.
            abbrev: The abbreviation of the telescope
            code: The one-character TEMPO-recognized site code
                for the telescope.

        Outputs:
            None
    """
    select = db.select([db.telescopes]).\
                where((db.telescopes.c.telescope_name==name) |
                      (db.telescopes.c.telescope_abbrev==abbrev) |
                      (db.telescopes.c.telescope_code==code)).\
                order_by(db.telescopes.c.telescope_id.asc())
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if rows:
        errormsg = "The proposed telescope name (%s), abbreviation (%s) " \
                    "and/or code (%s) clashes with the following telescope " \
                    "entries in the DB: " % (name, abbrev, code)
        for row in rows:
            errormsg += "\n    - ID: %(telescope_id)d, " \
                                "Name: %(telescope_name)s, " \
                                "Abbrev: %(telescope_abbrev)s, " \
                                "Code: %(telescope_code)s" % row
        raise errors.BadInputError(errormsg)


def validate_aliases(db, aliases):
    """Check if any of the given telescope aliases are already in use.
        If so, raise errors.BadInputError.

        Inputs:
            db: A connected Database object.
            aliases: A list of telescope aliases.

        Outputs:
            None
    """
    select = db.select([db.telescope_aliases]).\
                where(db.telescope_aliases.c.telescope_alias.in_(aliases))
    result = db.execute(select)
    aliases_in_use = []
    for row in result:
        aliases_in_use.append(row['telescope_alias'])
    result.close()
    if aliases_in_use:
        raise errors.BadInputError("The following proposed telescope aliases " \
                                    "are already in use (each alias must be " \
                                    "unique): '%s'" % \
                                    "', '".join(aliases_in_use))


def add_telescope(db, name, itrf_x, itrf_y, itrf_z, abbrev, code, aliases=[]):
    """Add a new telescope and its aliases to the database.

        Inputs:
            db: A connected Database object.
            name: The name of the telescope.
            itrf_x: The x-coordinate of the telescope, as is used
                in TEMPO's obsys.dat.
            itrf_y: The y-coordinate of the telescope, as is used
                in TEMPO's obsys.dat.
            itrf_z: The z-coordinate of the telescope, as is used
                in TEMPO's obsys.dat.
            abbrev: The abbreviated name of the telescope.
            code: The TEMPO-recognized site code for this telescope.
            aliases: A list of alternative names for this telescope.
                (Default: no additional names).

        Output:
            telescope_id: The ID number of the newly inserted telescope.
    """
    # Add the telescope's name, abbrev, and code to the list of aliases
    aliases.extend([name, abbrev, code])
    # Convert aliases to lower case
    tmp = {}
    for a in aliases:
        tmp[a.lower()] = a
    aliases = tmp.values()

    db.begin() # Open a transaction
    try:
        validate_telescope(db, name, abbrev, code)
        validate_aliases(db, aliases)
    except errors.BadInputError:
        db.rollback()
        raise
    # Insert new telescope into the database
    ins = db.telescopes.insert()
    values = {'telescope_name':name, \
              'itrf_x':itrf_x, \
              'itrf_y':itrf_y, \
              'itrf_z':itrf_z, \
              'telescope_abbrev':abbrev, \
              'telescope_code':code}
    result = db.execute(ins, values)
    telescope_id = result.inserted_primary_key[0]
    result.close()

    # Insert new aliases into the database
    ins = db.telescope_aliases.insert()
    values = []
    for alias in aliases:
        values.append({'telescope_id':telescope_id, \
                        'telescope_alias':alias})
    result = db.execute(ins, values)
    result.close()

    db.commit()
    return telescope_id


def main():
    db = database.Database()
    db.connect()

    try:
        telescope_id = add_telescope(db, args.name, args.itrf_x, \
                                        args.itrf_y, args.itrf_x, \
                                        args.abbrev, args.code, \
                                        args.aliases)
        print "Successfully inserted new telescope. " \
                    "Returned telescope_id: %d" % telescope_id
    finally:
        db.close()


if __name__=='__main__':
    parser = utils.DefaultArguments(description="Add a new telescope to the DB")
    parser.add_argument('-t', '--telescope-name', dest='name', \
                        type=str, required=True, \
                        help="The preferred name of the new telescope. " \
                            "NOTE: This is required.")
    parser.add_argument('-x', dest='itrf_x', \
                        type=float, required=True, \
                        help="The x-coordinate of the telescope, as is " \
                            "provided in TEMPO's obsys.dat. NOTE: This " \
                            "is required.")
    parser.add_argument('-y', dest='itrf_y', \
                        type=float, required=True, \
                        help="The y-coordinate of the telescope, as is " \
                            "provided in TEMPO's obsys.dat. NOTE: This " \
                            "is required.")
    parser.add_argument('-z', dest='itrf_z', \
                        type=float, required=True, \
                        help="The z-coordinate of the telescope, as is " \
                            "provided in TEMPO's obsys.dat. NOTE: This " \
                            "is required.")
    parser.add_argument('-s', '--abbrev', dest='abbrev', \
                        type=str, required=True, \
                        help="The abbreviated name of the telescope. " \
                            "NOTE: This is required.")
    parser.add_argument('-c', '--code', dest='code', \
                        type=str, required=True, \
                        help="The TEMPO-recognized site-code of this " \
                            "telescope. NOTE: This is required.")
    parser.add_argument('-a', '--alias', dest='aliases', \
                        type=str, action='append', default=[], \
                        help="An alias for the telescope. NOTE: multiple " \
                            "aliases may be provided by including " \
                            "multiple -a/--alias flags.")
    args = parser.parse_args()
    main()
