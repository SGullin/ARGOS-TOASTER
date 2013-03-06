#!/usr/bin/env python
"""Edit a timfile comment or add/remove TOAs.
"""


import database
import errors
import utils
from toolkit.timfiles import conflict_handlers

SHORTNAME = 'edit'
DESCRIPTION = "Edit a timfile comment or add/remove TOAs."


def add_arguments(parser):
    parser.add_argument('-t', '--timfile-id', dest='timfile_id', \
                        required=True, type=int, \
                        help="The ID of the timefile entry in the DB to " \
                            "edit. NOTE: This is required.")
    parser.add_argument('--comments', dest='comments', type=str, \
                        help="Provide comments for the timfile.")
    parser.add_argument('-a', '--add-toa', dest='toas_to_add', \
                        action='append', type=int, default=[], \
                        metavar="TOA_ID", \
                        help="The ID of a TOA to add to the timfile. " \
                            "NOTE: multiple -a/--add-toa options may be " \
                            "provided.")
    parser.add_argument('-r', '--rm-toa', dest='toas_to_remove', \
                        action='append', type=int, default=[], \
                        metavar="TOA_ID", \
                        help="The ID of a TOA to remove from the timfile." \
                            "NOTE: multiple -r/p-rm-toa options may be " \
                            "provided.")

def verify_timfile(timfile_id, existdb=None):
    """Verify TOAs in timfile do don't have any conflicts.
        The strict conflict handler is used (i.e. errors will be 
        raised if any conflicts are found).

        Inputs:
            timfile_id: The ID of the timfile to verify.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Outputs:
            None
    """
    db = existdb or database.Database()
    db.connect()

    select = db.select([db.toas, \
                        db.process.c.parfile_id, \
                        db.replacement_rawfiles.c.replacement_rawfile_id], \
                from_obj=[db.toa_tim. \
                    outerjoin(db.toas, \
                        onclause=db.toas.c.toa_id == db.toa_tim.c.toa_id).\
                    outerjoin(db.process, \
                        onclause=db.toas.c.process_id == \
                                db.process.c.process_id).\
                    outerjoin(db.replacement_rawfiles, \
                        onclause=db.toas.c.rawfile_id == \
                                db.replacement_rawfiles.c.obsolete_rawfile_id)]).\
                where(db.toa_tim.c.timfile_id==timfile_id)
    result = db.execute(select)
    toas = result.fetchall()

    try:
        toas = conflict_handlers.strict_conflict_handler(toas)
    finally:
        if not existdb:
            db.close()


def __add_toas(timfile_id, toas_to_add, existdb=None):
    """Add TOAs to timfile.
        
        Inputs:
            timfile_id: The ID of the timfile to verify.
            toas_to_add: List of toa_ids to add.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Outputs:
            None
    """
    db = existdb or database.Database()
    db.connect()

    ins = db.toa_tim.insert()
    values = []
    for toa_id in toas_to_add:
        values.append({'timfile_id':timfile_id, \
                       'toa_id':toa_id})
    result = db.execute(ins, values)
    result.close()

    if not existdb:
        db.close()


def __remove_toas(timfile_id, toas_to_remove, existdb=None):
    """Remove TOAs from timfile.
        
        Inputs:
            timfile_id: The ID of the timfile to verify.
            toas_to_remove: List of toa_ids to remove.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Outputs:
            None
    """
    db = existdb or database.Database()
    db.connect()
   
    delete = db.toa_tim.delete().\
                where((db.toa_tim.c.timfile_id==timfile_id) & \
                        (db.toa_tim.c.toa_id.in_(toas_to_remove)))
    result = db.execute(delete)
    result.close()

    if not existdb:
        db.close()

def __update_comments(timfile_id, comments, existdb=None):
    """Replace the timfile's comments.
        
        Input:
            timfile_id: The ID of the timfile to verify.
            comments: The timfile's new comments.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Outputs:
            None
    """
    if (comments is None) and (not toas_to_add) and (not toas_to_remove):
        errors.BadInputError("No edits provided. Please either provide " \
                    "a comment, TOAs to add, or TOAs to remove.")
    if comments is not None and not comments:
        errors.BadInputError("The timfile's comment cannot be blank.")
    
    db = existdb or database.Database()
    db.connect()
    
    values = {'comments': comments}
    update = db.timfiles.update().\
            where(db.timfiles.c.timfile_id == timfile_id)
    results = db.execute(update, values)
    results.close()
    
    if not existdb:
        db.close()


def edit_timfile(timfile_id, toas_to_add=[], toas_to_remove=[], \
                comments=None, existdb=None):
    """Edit a timfile. This function can be used to add/remove toas, 
        or update the comment.

        Inputs:
            timfile_id: The ID of the timfile to edit.
            toas_to_add: List of toa_ids to add. (Default: Don't
                add any TOAs.)
            toas_to_remove: List of toa_ids to remove. (Default: 
                Don't remove any TOAs.)
            comment: The timfile's new comment. (Default: Don't update
                the timfile's comment.)
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Outputs:
            None
    """
    
    # Use sets to remove duplicates
    toas_to_remove = set(toas_to_remove)
    toas_to_add = set(toas_to_add)

    add_and_remove = set.intersection(toas_to_remove, toas_to_add)
    if add_and_remove:
        toastr = ", ".join([str(x) for x in sorted(add_and_remove)])
        raise errors.BadInputError("Some TOAs are scheduled for addition " \
                        "and removal (TOA IDs: %s)! Please remove " \
                        "ambiguities." % toastr)

    # Connect to the database
    db = existdb or database.Database()
    db.connect()
    trans = db.begin()
    try:
        if toas_to_remove:
            __remove_toas(timfile_id, toas_to_remove, existdb=db)
        if toas_to_add:
            __add_toas(timfile_id, toas_to_add, existdb=db)
        __update_comments(timfile_id, comments, existdb=db)
        verify_timfile(timfile_id, existdb=db)
    except:
        trans.rollback()
        raise
    else:
        trans.commit()
    finally:
        if existdb is None:
            db.close()


def main(args):
    if args.timfile_id is None:
        raise errors.BadInputError("No timfile identified. Use '--timfile-id' " \
                        "command line argument.")
    edit_timfile(args.timfile_id, args.toas_to_add, args.toas_to_remove, \
                    args.comments)


if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args=parser.parse_args()
    main(args)
