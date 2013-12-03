#!/usr/bin/env python

SHORTNAME = "comment"
DESCRIPTION = "Comment on a TOA, or flag a TOA as good/bad."


def add_arguments(parser):
    parser.add_argument('--toa-id', dest='toa_id', type=int, \
                        help='Individual TOA ID to include.')
    parser.add_argument('--good', dest='flag', default=None, \
                        action='store_false', \
                        help="Flag TOA as good.")
    parser.add_argument('--bad', dest='is_bad', default=None, \
                        action='store_true', \
                        help="Flag TOA as bad.")
    parser.add_argument('--comments', dest='comments', type=str, \
                        help="Provide comments for the TOA.")
    parser.add_argument('-f', '--force', dest='force', \
                        action='store_true', default=False, \
                        help="Forcefully set opinion even if one " \
                            "already exists in DB.")


def main(args):
    user_id = utils.get_userid()
    username = utils.get_username(user_id)

    if args.comments is None and args.is_bad is None:
        raise errors.BadInputError("You have no opinion. Please provide a " \
                        "comment or explicity specify if TOA is good/bad!")
    if args.toa_id is None:
        raise erros.BadInputError("No TOA provided. Use '--toa-id' command " \
                        "line argument.")
    if args.comments is None:
        warnings.warn("No comment for TOA (ID: %d) provided." % \
                        args.toa_id, errors.ToasterError)
    if not args.comments:
        warnings.warn("Comment for TOA (ID: %d) is explicitly blank." % \
                            args.toa_id, errors.ToasterError)

    # Connect to the database
    db = existdb or database.Database()
    db.connect()
    trans = db.begin()
    try:
        # Check to see if user already has a comment/flag
        select = db.select([db.toa_opinions.c.comments, \
                            db.toa_opinions.c.is_bad]).\
                    where(db.toa_opinions.c.toa_id == args.toa_id &
                            db.toa_opinions.c.user_id == user_id)
        results = db.execute(select)
        rows = results.fetchall()
        results.close()
        if len(rows) == 1:
            # Are we authorised to replace existing opinons?
            if args.force:
                # Update existing opinon
                query= db.toa_opinions.update().\
                            where(db.toa_opinions.c.toa_id == args.toa_id & \
                                db.toa_opinions.c.user_id == user_id)
            else:
                # Raise exception
                raise errors.BadInputError("User '%s' (ID: %d) already has " \
                        "an opinion for this TOA in the database (Comment: " \
                        "%s; TOA is %s). Need permission to replace this " \
                        "opinion forcefully." % \
                        (username, user_id, rows[0].comments, \
                            ((rows[0].is_bad) and "bad") or "good"))
        elif len(rows):
            raise errors.InconsistentDatabaseError("There are multiple " \
                        "opinions for this TOA (ID: %d) from the user " \
                        "'%s' (ID: %d)!" % \
                        (args.toa_id, username, user_id))
        else:
            query = db.toa_opinions.insert()
        values = {'is_bad': args.is_bad}
        if not args.comments:
             # Set comments to NULL
             values['comments'] = None
        results = db.execute(values)
        results.close()
    except:
        trans.rollback()
        raise
    else:
        trans.commit()
    finally:
        if existdb is None:
            db.close()
    

if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
