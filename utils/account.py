import pwd

from toaster import database
from toaster import errors

from toaster.utils import cache

def is_admin(user_id, existdb=None):
    """Return whether user has administrator privileges or not.

        Input:
            user_id: The ID of the user to check privileges for.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            admin: True, if the user has admin privileges. False, otherwise.
    """
    # Connect to the DB if necessary
    db = existdb or database.Database()
    db.connect()
   
    select = db.select([db.users.c.admin]).\
                where((db.users.c.user_id == user_id) & \
                        db.users.c.active)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()

    if len(rows) == 1:
        admin = rows[0]['admin']
    elif len(rows) > 1:
        raise errors.InconsistentDatabaseError("Multiple rows (%d) with " \
                                "user_id=%d!" % (len(rows), user_id))
    else:
        raise errors.UnrecognizedValueError("User ID (%d) is not " \
                                "recognized!" % user_id)
    return admin


def is_curator(user_id, pulsar_id, existdb=None):
    """Return whether user has curator privileges for the given
        pulsar.

        Inputs:
            user_id: The ID of the user to check privileges for.
            pulsar_id: The ID of the pulsar in question.
            existdb: A (optional) existing database connection object.
                (Default: Establish a db connection)

        Output:
            curator: True if the user has curator privileges. 
                False otherwise.
    """
    # Check if user_id and pulsar_id are valid
    # Exceptions will be raise if no matches are found
    cache.get_userinfo(user_id)
    cache.get_pulsarname(pulsar_id)

    # Connect to the DB if necessary
    db = existdb or database.Database()
    db.connect()
    select = db.select([db.curators.c.user_id], \
                from_obj=[db.curators.\
                    outerjoin(db.users, \
                        onclause=db.curators.c.user_id == \
                                    db.users.c.user_id)]).\
                where((db.curators.c.pulsar_id == pulsar_id) & \
                        db.curators.c.user_id.in_((user_id,None)) & \
                        db.users.c.active)
    result = db.execute(select)
    rows = result.fetchall()
    result.close()

    curator = bool(rows)
    return curator


