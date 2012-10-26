#!/usr/bin/env python
import os.path
import traceback
import copy
import shlex

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
        if args.from_file is not None:
            if args.from_file == '-':
                tellist = sys.stdin
            else:
                if not os.path.exists(args.from_file):
                    raise errors.FileError("The telescope list (%s) does " \
                                "not appear to exist." % args.from_file)
                tellist = open(args.from_file, 'r')
            numfails = 0
            numadded = 0
            for line in tellist:
                # Strip comments
                line = line.partition('#')[0].strip()
                if not line:
                    # Skip empty line
                    continue
                try:
                    customargs = copy.deepcopy(args)
                    arglist = shlex.split(line.strip())
                    parser.parse_args(arglist, namespace=customargs)
        
                    if customargs.name is None or customargs.itrf_x is None or \
                            customargs.itrf_y is None or customargs.itrf_z is None or \
                            customargs.abbrev is None or customargs.code is None:
                        raise errors.BadInputError("Telescopes " \
                                "must have a name, IRTF coordinates (X,Y,Z), " \
                                "an abbreviation, and a site code. " \
                                "One of these is missing.")
                    telescope_id = add_telescope(db, customargs.name, \
                                        customargs.itrf_x, customargs.itrf_y, \
                                        customargs.itrf_x, customargs.abbrev, \
                                        customargs.code, customargs.aliases)
                    print "Successfully inserted new telescope. " \
                               "Returned telescope_id: %d" % telescope_id
                    numadded += 1
                except errors.ToasterError:
                    numfails += 1
                    traceback.print_exc()
            if args.from_file != '-':
                tellist.close()
            print "\n\n===================================\n" \
                      "%d telescopes successfully added\n" \
                      "===================================\n" % numadded
            if numfails:
                raise errors.ToasterError(\
                    "\n\n===================================\n" \
                        "The adding of %d telescopes failed!\n" \
                        "Please review error output.\n" \
                        "===================================\n" % numfails)
        else:
            telescope_id = add_telescope(db, customargs.name, \
                                customargs.itrf_x, customargs.itrf_y, \
                                customargs.itrf_x, customargs.abbrev, \
                                customargs.code, customargs.aliases)
            print "Successfully inserted new telescope. " \
                       "Returned telescope_id: %d" % telescope_id
    finally:
        db.close()


if __name__=='__main__':
    parser = utils.DefaultArguments(description="Add a new telescope to the DB")
    parser.add_argument('-t', '--telescope-name', dest='name', \
                        type=str, \
                        help="The preferred name of the new telescope.")
    parser.add_argument('-x', dest='itrf_x', \
                        type=float, \
                        help="The x-coordinate of the telescope, as is " \
                            "provided in TEMPO's obsys.dat.")
    parser.add_argument('-y', dest='itrf_y', \
                        type=float, \
                        help="The y-coordinate of the telescope, as is " \
                            "provided in TEMPO's obsys.dat.")
    parser.add_argument('-z', dest='itrf_z', \
                        type=float, \
                        help="The z-coordinate of the telescope, as is " \
                            "provided in TEMPO's obsys.dat.")
    parser.add_argument('-s', '--abbrev', dest='abbrev', \
                        type=str, \
                        help="The abbreviated name of the telescope.")
    parser.add_argument('-c', '--code', dest='code', \
                        type=str, \
                        help="The TEMPO-recognized site-code of this " \
                            "telescope.")
    parser.add_argument('-a', '--alias', dest='aliases', \
                        type=str, action='append', default=[], \
                        help="An alias for the telescope. NOTE: multiple " \
                            "aliases may be provided by including " \
                            "multiple -a/--alias flags.")
    parser.add_argument('--from-file', dest='from_file', \
                        type=str, default=None, \
                        help="A list of telescopes (one per line) to " \
                            "add. Note: each line can also include " \
                            "alias flags. (Default: load a single " \
                            "telescope given on the cmd line.)")
    args = parser.parse_args()
    main()
