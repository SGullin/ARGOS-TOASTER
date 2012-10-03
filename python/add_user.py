#!/usr/bin/env python
import re
import sys
import getpass

import epta_pipeline_utils as epu
import database
import errors

class HashPasswordAction(epu.argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, epu.hash_password(values))


def add_new_user(db, user_name, real_name, email_address, passwd_hash):
    """Add a new user to the database.
        
        Inputs:
            db: A connected Database object.
            user_name: The user name.
            real_name: The user's real name.
            email_address: The user's email address.
            passwd_hash: The MD5 hash of the user's password.

        Output:
            user_id: The user_id of the new user.
    """
    trans = db.begin() # Open a transaction
    try:
        validate_proposed_user(db, user_name, real_name, email_address)
    except errors.BadInputError:
        db.rollback()
        raise
    # Insert new user into the database
    ins = db.users.insert()
    values = {'user_name':user_name, \
               'real_name':real_name, \
               'email_address':email_address, \
               'passwd_hash':passwd_hash}
    result = db.execute(ins, values)
    user_id = result.inserted_primary_key[0]
    result.close()
    db.commit()
    return user_id


def validate_proposed_user(db, user_name, real_name, email_address):
    """Check if the proposed user can be added to the database.
        Raises errors.BadInputError if the user_name, real_name,
        or email_address are already in use.

        Inputs:
            db: A connected Database object.
            user_name: The user name.
            real_name: The user's real name.
            email_address: The user's email address.

        Outputs:
            None
    """
    select = db.select([db.users], \
                        (db.users.c.user_name==user_name) | \
                        (db.users.c.real_name==real_name) | \
                        (db.users.c.email_address==email_address))
    result = db.execute(select)
    row = result.fetchone()
    result.close()
    if row is not None:
        raise errors.BadInputError("Cannot add user (User name: %s, " \
                                    "Real name: %s, Email address: %s) " \
                                    "because one or more of these values " \
                                    "is already in use." % \
                    (user_name, real_name, email_address))


def main():
    db = database.Database()
    db.connect()

    if args.passwd_hash is None:
        # Sloppily pre-check to make sure we don't ask for a password
        # for an account that will not be valid. Note that since we're 
        # not locking the DB table, it's possible the account becomes 
        # invalid between this check and the actual insert. We do, 
        # however re-check (properly, using transactions) if the account 
        # is valid at insert-time.
        validate_proposed_user(db, args.user_name, args.real_name, \
                                args.email_address)
    # Get password interactively, if necessary
    while args.passwd_hash is None:
        # No password (or hash) provided on command line
        # Ask user for password
        hash1 = epu.hash_password(getpass.getpass("Password for %s: " % \
                                            args.user_name))
        hash2 = epu.hash_password(getpass.getpass("Re-type password: "))
        if hash1 == hash2:
            args.passwd_hash = hash1
        else:
            sys.stderr.write("Passwords don't match. Try again.\n")
    
    user_id = add_new_user(db, args.user_name, args.real_name, \
                            args.email_address, args.passwd_hash)
    print "Successfully inserted new user. " \
                "Returned user_id: %d" % user_id


if __name__=='__main__':
    parser = epu.DefaultArguments(description="Add a new user to the DB.")
    parser.add_argument('-u', '--user-name', dest='user_name', \
                        type=str, required=True, \
                        help="The new user's username. NOTE: This is required.")
    parser.add_argument('-n', '--real-name', dest='real_name', \
                        type=str, required=True, \
                        help="The new user's name. NOTE: This is required.")
    parser.add_argument('-e', '--email', dest='email_address', \
                        type=str, required=True, \
                        help="The new user's email address. NOTE: This " \
                            "is required.")
    pwgroup = parser.add_mutually_exclusive_group(required=False)
    pwgroup.add_argument('-p', '--password', dest='passwd_hash',
                        type=str, action=HashPasswordAction, \
                        help="The new user's password. NOTE: Providing a " \
                            "password using this flag will require typing " \
                            "it on the command line, which will probably " \
                            "log it somewhere in plain text.")
    pwgroup.add_argument('--passhash', dest='passwd_hash', type=str, \
                        help="The MD5 hash of the new user's password.")
    args = parser.parse_args()
    main()
