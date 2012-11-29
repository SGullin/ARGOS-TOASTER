#!/usr/bin/env python
"""
A script to replace one data archive with another.
The replaced data archive is not removed from the database.

Patrick Lazarus, Nov. 17, 2012
"""

import utils
import database
import load_rawfile


def verify_replacement(obsolete_id, replacement, db):
    """Verify that the replacement file is a suitable replacement
        for the obsolete file. The following conditions must be
        satisfied:
            - Both observations refer to the same pulsar 
                (i.e. same pulsar_id)
            - Both observations come from the same observing system 
                (i.e. same obssystem_id)
            - The observations overlap 
                (this is checked by comparing the start/end MJDs)

        Inputs:
            obsolete_id: The rawfile_id value of the file being replaced.
            replacement: The name of the replacement file.
            db: An connected database object.

        Outputs:
            None
    """
    # Get info about the replacement file from its header
    replaceparams = utils.prep_file(replacement)

    # Get info about the obsolete file from database
    select = db.select([db.rawfiles.c.pulsar_id, \
                        db.rawfiles.c.obssystem_id, \
                        db.rawfiles.c.mjd, \
                        db.rawfiles.c.length]).\
                where(db.rawfiles.c.rawfile_id==obsolete_id)
    result = db.execute(select)
    rows = result.fetchall()
    if len(rows) > 1:
        raise errors.InconsistentDatabaseError("Multiple matches (%s) " \
                    "for rawfile_id (%d)! This should not happen..." % \
                    (len(rows), obsolete_id))
    elif len(rows) < 1:
        raise BadInputError("The rawfile_id provided (%d) does not " \
                    "exist!" % obsolete_id)
    else:
        obsoleteparams = rows[0]

    # Now check that the obsolete file and its replacement are compatible
    # Check the replacement is from the same obssystem
    if obsoleteparams['obssystem_id'] != replaceparams['obssystem_id']:
        raise errors.FileError("The observing system of the replacement " \
                    "(ID: %d) doesn't match the observing system of " \
                    "the file it's replacing (ID: %d)." % \
                    (obsoleteparams['obssystem_id'], \
                        replaceparams['obssystem_id']))
    # Check the replacement is data on the same pulsar
    if obsoleteparams['pulsar_id'] != replaceparams['pulsar_id']:
        raise errors.FileError("The pulsar name in the replacement file " \
                    "(%s) doesn't match the pulsar name in the file it's "
                    "replacing (%s)." % \
                    (utils.get_pulsarname(obsoleteparams['pulsar_id']), \
                        utils.get_pulsarname(replaceparams['pulsar_id'])))
    # Check the replacement overlaps with the obsolete file
    omjdstart = obsoleteparams['mjd']
    omjdend = omjdstart + obsoleteparams['length']/86400.0
    rmjdstart = replaceparams['mjd']
    rmjdend = rmjdstart + replaceparams['length']/86400.0
    if (omjdstart >= rmjdend or omjdend <= rmjdstart):
        raise errors.FileError("The replacement file (MJD: %f - %f) " \
                    "doesn't overlap with the file it is replacing " \
                    "(MJD: %f - %f)." % \
                    (rmjdstart, rmjdend, omjdstart, omjdend))


def replace_rawfile(obsolete_id, replace_id, comments, existdb=None):
    """In the database, mark an obsolete data file as being replaced.

        Inputs:
            obsolete_id: The rawfile_id of the data file being replaced.
            replace_id: The rawfile_id of the replacement data file.
            comments: A comment describing the replacement.
            existdb: An (optional) existing database connection object.
                (Default: Establish a db connection)
        Outputs:
            None
    """
    # Connect to the database
    db = existdb or database.Database()
    db.connect()

    # Check if obsolete_id exists in rawfiles. If not, fail.
    select = db.select([db.rawfiles.c.rawfile_id, \
                        db.replacement_rawfiles.c.replacement_rawfile_id.\
                                label("existing_replace_id")], \
                from_obj=[db.rawfiles. \
                    outerjoin(db.replacement_rawfiles, \
                        onclause=db.replacement_rawfiles.c.obsolete_rawfile_id == \
                                db.rawfiles.c.rawfile_id)]).\
                where(db.rawfiles.c.rawfile_id == obsolete_id)
    result = db.execute(select)
    rows = result.fetchall()
    if len(rows) > 1:
        raise errors.InconsistentDatabaseError("There are multiple (%d) " \
                    "rawfiles with ID=%d. Each ID should be unique!" % \
                    (len(rows), obsolete_id))
    elif len(rows) != 1:
        raise errors.BadInputError("The obsolete rawfile being replaced " \
                    "(ID:%d) does not exist!" % obsolete_id)
    row = rows[0] # There is only one row

    # Check if obsolete_id is already replaced. If so, list replacement and fail.
    if row['existing_replace_id'] is not None:
        raise RawfileSuperseded("The rawfile (ID=%d) has already been " \
                                "replaced by ID=%d. Perhaps it is the " \
                                "latter file that should be replaced, or " \
                                "perhaps no additional replacement is " \
                                "required." % \
                                (obsolete_id, row['existing_replace_id']))

    # Log the replacement
    user_id = utils.get_userid()
    ins = db.replacement_rawfiles.insert()
    values = {'obsolete_rawfile_id':obsolete_id, \
              'replacement_rawfile_id':replace_id, \
              'user_id':user_id, \
              'comments':comments}
    result = db.execute(ins, values)
    result.close()
    
    # Check if obsolete_id is itself a replacement for other files
    # If so, mark all with newest replacement and
    # append comment (tag with date/time)?
    user = utils.get_userinfo()
    update = db.replacement_rawfiles.update().\
                where(db.replacement_rawfiles.c.replacement_rawfile_id == \
                            obsolete_id)
    values = {'replacement_rawfile_id':replace_id, \
              'comments':db.replacement_rawfiles.c.comments}
    results = db.execute(update, values)
    results.close()


def main():
    if not args.comments:
        raise errors.BadInputError("A comment describing/motivating " \
                    "the replacement must be provided!")

    # Connect to the database
    db = database.Database()
    db.connect()
    
    trans = db.begin() # Open a DB transaction
    try:
        verify_replacement(args.obsolete_id, args.replacement, db)
        replace_id = load_rawfile.load_rawfile(args.replacement, db)
        replace_rawfile(args.obsolete_id, replace_id, \
                            args.comments, db)
        utils.print_info("Successfully marked rawfile (ID: %d) and " \
                        "being superseded by a new rawfile (ID: %d)" % \
                        (args.obsolete_id, replace_id), 1)
    except:
        db.rollback()
        raise
    else:
        db.commit()
    finally:
        # Close DB connection
        db.close()


if __name__=='__main__':
    parser = utils.DefaultArguments(description="Replace a data file with " \
                                        "another. The obsolete file is not " \
                                        "removed from the database or archive.")
    parser.add_argument("replacement", nargs='?', type=str, \
                        help="File name of the new raw file to upload.")
    parser.add_argument("--obsolete-id", dest='obsolete_id', type=int, \
                        help="Rawfile ID number of the data file that " \
                            "is being replaced.")
    parser.add_argument('--comments', dest='comments', type=str, \
                        help="Provide comments describing why the replacement " \
                            "is being done.")
    args = parser.parse_args()
    main()
