#!/usr/bin/env python

import epta_pipeline_utils as epu
import database
import errors


def validate_obssystem(db, name, telescope_id, frontend, backend, clock):
    """Check if the given observing system is already in use.
        If so, raise errors.BadInputError.

        Inputs:
            db: A connected Database object.
            name: The name of the observing system.
            telescope_id: The DB's ID number for the telescope.
            frontend: The name of the frontend.
            backend: The name of the backend.
            clock: The clock file.

        Outputs:
            None
    """
    select = db.select([db.obssystems]).\
                where((db.obssystems.c.name==name) | \
                      ((db.obssystems.c.telescope_id==telescope_id) & \
                        (db.obssystems.c.frontend==frontend) & \
                        (db.obssystems.c.backend==backend) & \
                        (db.obssystems.c.clock==clock))).\
                order_by(db.obssystems.c.obssystem_id.asc())
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    if rows:
        errormsg = "The proposed observing system (name:%s, telescope_id:%d, " \
                    "frontend:%s, backend:%s, clock:%s) clashes with the " \
                    "following obssystem entries in the DB:\n" \
                    "(NOTE: All obssystem names must be unique. Also, " \
                    "the other values cannot be a complete duplicate of " \
                    "an existing entry.)" % \
                    (name, telescope_id, frontend, backend, clock)
        for row in rows:
            errormsg += "\n    - ID: %(obssystem_id)d,\n" \
                        "        Name: %(name)s,\n" \
                        "        Telescope ID: %(telescope_id)d,\n" \
                        "        Frontend: %(frontend)s,\n" \
                        "        Backend: %(backend)s,\n" \
                        "        Clock file: %(clock)s" % row
        raise errors.BadInputError(errormsg)


def get_telescope_info(db, alias):
    """Given a telescope alias return the info from the 
        matching telescope columns.

        Inputs:
            db: A connected Database object.
            alias: The telescope's alias.

        Output:
            rows: The matching rows (RowProxy objects).
    """
    select = db.select([db.telescopes.c.telescope_id, \
                        db.telescopes.c.telescope_name, \
                        db.telescopes.c.telescope_abbrev, \
                        db.telescopes.c.telescope_code], \
                from_obj=[db.telescopes.\
                    join(db.telescope_aliases, \
                    onclause=db.telescopes.c.telescope_id == \
                            db.telescope_aliases.c.telescope_id)], \
                distinct=db.telescopes.c.telescope_id).\
                where(db.telescope_aliases.c.telescope_alias.like(alias))
    result = db.execute(select)
    rows = result.fetchall()
    result.close()
    return rows


def add_obssystem(db, name, telescope_id, frontend, backend, clock):
    """Add a new observing system to the database.
        
        Inputs:
            db: A connected Database object.
            name: The name of the observing system.
            telescope_id: The DB's ID number for the telescope.
            frontend: The name of the frontend.
            backend: The name of the backend.
            clock: The clock file.

        Outputs:
            obssystem_id: The ID number of the newly inserted
                observing system.
    """
    db.begin() # Open a transaction
    try:
        validate_obssystem(db, name, telescope_id, frontend, \
                                backend, clock)
    except errors.BadInputError:
        db.rollback()
        raise
    # Insert new observing system into th databae
    ins = db.obssystems.insert()
    values = {'name':name, \
              'telescope_id':telescope_id, \
              'frontend':frontend, \
              'backend':backend, \
              'clock':clock}
    result = db.execute(ins, values)
    obssystem_id = result.inserted_primary_key[0]
    result.close()
    db.commit()
    return obssystem_id


def main():
    db = database.Database()
    db.connect()

    rows = get_telescope_info(db, args.telescope)
    if len(rows) > 1:
        raise errors.BadInputError("Multiple matches (%d) for this " \
                                    "telescope alias (%s)! Be more " \
                                    "specific." % (len(rows), args.telescope))
    row = rows[0]
    telescope_id = row['telescope_id']

    if args.name is None:
        args.name = "%s_%s_%s" % (row['telescope_abbrev'].upper(), \
                                    args.backend.upper(), \
                                    args.frontend.upper())
    try:
        obssystem_id = add_obssystem(db, args.name, telescope_id, \
                            args.frontend, args.backend, args.clock)
        print "Successfully inserted new observing system. " \
                    "Returned obssystem_id: %d" % obssystem_id
    finally:
        db.close()


if __name__ =='__main__':
    parser = epu.DefaultArguments(description="Add a new observing system " \
                                                "to the DB")
    parser.add_argument('-o', '--obssys-name', dest='name', type=str, \
                        help="The name of the new observing system. " \
                            "(Default: Generate a name from the telescope, " \
                            "frontend and backend).")
    parser.add_argument('-t', '--telescope', dest='telescope', \
                        type=str, required=True, \
                        help="The name of the telescope. This could be " \
                            "an alias. NOTE: This is required.")
    parser.add_argument('-f', '--frontend', dest='frontend', \
                        type=str, required=True, \
                        help="The name of the frontend. " \
                            "NOTE: This is required.")
    parser.add_argument('-b', '--backend', dest='backend', \
                        type=str, required=True, \
                        help="The name of the backend. " \
                            "NOTE: This is required.")
    parser.add_argument('-c', '--clock', dest='clock', \
                        type=str, required=True, \
                        help="The name of the clock file. " \
                            "NOTE: This is required.")
    args = parser.parse_args()
    main()
