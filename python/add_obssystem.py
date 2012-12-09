#!/usr/bin/env python
import sys
import os.path
import traceback
import copy
import shlex

import utils
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
                        "        Observing band: %(band_descriptor)s,\n" \
                        "        Clock file: %(clock)s" % row
        raise errors.BadInputError(errormsg)


def add_obssystem(db, name, telescope_id, frontend, backend, band, clock):
    """Add a new observing system to the database.
        
        Inputs:
            db: A connected Database object.
            name: The name of the observing system.
            telescope_id: The DB's ID number for the telescope.
            frontend: The name of the frontend.
            backend: The name of the backend.
            band: The name of the observing band.
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
              'band_descriptor':band, \
              'clock':clock}
    result = db.execute(ins, values)
    obssystem_id = result.inserted_primary_key[0]
    result.close()
    db.commit()
    return obssystem_id


def main():
    # Connect to the database
    db = database.Database()
    db.connect()

    try:
        if args.from_file is not None:
            if args.from_file == '-':
                obssyslist = sys.stdin
            else:
                if not os.path.exists(args.from_file):
                    raise errors.FileError("The obssystem list (%s) does " \
                                "not appear to exist." % args.from_file)
                obssyslist = open(args.from_file, 'r')
            numfails = 0
            numadded = 0
            for line in obssyslist:
                # Strip comments
                line = line.partition('#')[0].strip()
                if not line:
                    # Skip empty line
                    continue
                try:
                    customargs = copy.deepcopy(args)
                    arglist = shlex.split(line.strip())
                    parser.parse_args(arglist, namespace=customargs)
        
                    if customargs.telescope is None or customargs.backend is None or \
                            customargs.frontend is None or customargs.band is None or \
                            customargs.clock is None:
                        raise errors.BadInputError("Observing systems " \
                                "must have a telescope, backend, frontend, " \
                                "band descriptor, and clock file! At least " \
                                "one of these is missing.")
                    tinfo = utils.get_telescope_info(customargs.telescope, db)
                    telescope_id = tinfo['telescope_id']

                    if customargs.name is None:
                        customargs.name = "%s_%s_%s" % \
                                    (tinfo['telescope_abbrev'].upper(), \
                                     customargs.backend.upper(), \
                                     customargs.frontend.upper())
                    
                    obssystem_id = add_obssystem(db, customargs.name, telescope_id, \
                                    customargs.frontend, customargs.backend, \
                                    customargs.band, customargs.clock)
                    print "Successfully inserted new observing system. " \
                            "Returned obssystem_id: %d" % obssystem_id
                    numadded += 1
                except errors.ToasterError:
                    numfails += 1
                    traceback.print_exc()
            if args.from_file != '-':
                obssyslist.close()
            if numadded:
                utils.print_success("\n\n===================================\n" \
                                    "%d obssystems successfully added\n" \
                                    "===================================\n" % numadded)
            if numfails:
                raise errors.ToasterError(\
                    "\n\n===================================\n" \
                        "The adding of %d obssystems failed!\n" \
                        "Please review error output.\n" \
                        "===================================\n" % numfails)
        else:
            if args.telescope is None or args.backend is None or \
                    args.frontend is None or args.band is None or \
                    args.clock is None:
                raise errors.BadInputError("Observing systems " \
                        "must have a telescope, backend, frontend, " \
                        "band descriptor, and clock file! At least " \
                        "one of these is missing.")
            tinfo = utils.get_telescope_info(args.telescope, db)
            telescope_id = tinfo['telescope_id']

            if args.name is None:
                args.name = "%s_%s_%s" % \
                            (tinfo['telescope_abbrev'].upper(), \
                             args.backend.upper(), \
                             args.frontend.upper())
            obssystem_id = add_obssystem(db, args.name, telescope_id, \
                        args.frontend, args.backend, args.band, args.clock)
            print "Successfully inserted new observing system. " \
                        "Returned obssystem_id: %d" % obssystem_id
    finally:
        db.close()


if __name__ =='__main__':
    parser = utils.DefaultArguments(description="Add a new observing system " \
                                                "to the DB")
    parser.add_argument('-o', '--obssys-name', dest='name', type=str, \
                        help="The name of the new observing system. " \
                            "(Default: Generate a name from the telescope, " \
                            "frontend and backend).")
    parser.add_argument('-t', '--telescope', dest='telescope', \
                        type=str, \
                        help="The name of the telescope. This can be " \
                            "an alias.")
    parser.add_argument('-f', '--frontend', dest='frontend', \
                        type=str, \
                        help="The name of the frontend.")
    parser.add_argument('-b', '--backend', dest='backend', \
                        type=str, \
                        help="The name of the backend.")
    parser.add_argument('-B', '--band-descriptor', dest='band', \
                        type=str, \
                        help="The name of the observing band.")
    parser.add_argument('-c', '--clock', dest='clock', \
                        type=str, \
                        help="The name of the clock file.")
    parser.add_argument('--from-file', dest='from_file', \
                        type=str, default=None, \
                        help="A list of obssystems (one per line) to " \
                            "add. Note: each line can also include " \
                            "alias flags. (Default: load a single " \
                            "obssystem given on the cmd line.)") 
    args = parser.parse_args()
    main()
