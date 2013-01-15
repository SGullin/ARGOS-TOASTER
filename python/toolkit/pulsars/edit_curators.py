#!/usr/bin/env python

"""
This TOASTER utility script adds/removes curators.

Patrick Lazarus, Jan 13, 2012.
"""
import config
import utils

SHORTNAME = 'curators'
DESCRIPTION = "Edit the list of curators for a pulsar. Note: If " \
              "a removal contradicts an additions, the removal " \
              "will take precedence."


def add_arguments(parser):
    parser.add_argument('-p', '--psr', dest='psrname', \
                        type=str, required=True, \
                        help="The pulsar to add/remove curators for.")
    parser.add_argument('--add', dest='to_add', \
                        type=str, default=[], action='append', \
                        help="User to add as a curator. The username " \
                            "should be provided.")
    parser.add_argument('--remove', dest='to_remove', \
                        type=str, default=[], action='append', \
                        help="User to remove as a curator. The username " \
                            "should be provided.")
    parser.add_argument('--clear', dest='remove_all',
                        action='store_true', default=False, \
                        help="Remove all curators.")
    parser.add_argument('--add-any', dest='add_wild', \
                        action='store_true', default=False, \
                        help="Add wildcard to curator list, allowing " \
                            "any user to have curator-level permissions.")
    parser.add_argument('--remove-any', dest='remove_wild', \
                        action='store_true', default=False, \
                        help="Remove wildcard from curator list, allowing " \
                            "any user to have curator-level permissions.")


def update_curators(pulsar_id, to_add_ids=[], to_rm_ids=[]):
    """Update the list of curators for the given pulsar.
        Note: if a user is specified to be added and removed,
            the removal will take precedence.

        Inputs:
            pulsar_id: The ID of the pulsar to edit curators for.
            to_add_ids: List of user IDs to add as curators.
            to_rm_ids: List of user IDs to remove as curators.

        Outputs:
            None
    """
    to_add_ids = set(to_add_ids)
    to_rm_ids = set(to_rm_ids)
    to_add_ids.difference_update(to_rm_ids) # Remove user_ids that will
                                            # lose curator privileges

    if config.cfg.verbosity >= 2:
        msg = "Updating curator privileges for %s" % \
                    utils.get_pulsarname(pulsar_id)
        for uid in to_add_ids:
            if uid is None:
                msg += "\n    + Wildcard"
            else:
                msg += "\n    + %s" % utils.get_userinfo(uid)['real_name']
        for uid in to_rm_ids:
            if uid is None:
                msg += "\n    - Wildcard"
            else:
                msg += "\n    - %s" % utils.get_userinfo(uid)['real_name']
        utils.print_info(msg, 2)
    raise NotImplementedError


def main(args):
    psr_id = utils.get_pulsarid(args.psrname)
    if args.remove_all:
        utils.print_info("Removing all curators for %s" % \
                            utils.get_pulsarname(psr_id), 2)
    else:
        to_add_ids = [utils.get_userid(username) for username in \
                            args.to_add]
        if args.add_wild:
            to_add_ids.append(None)

        to_rm_ids = [utils.get_userid(username) for username in \
                            args.to_remove]
        if args.remove_wild:
            to_rm_ids.append(None)
        update_curators(psr_id, to_add_ids, to_rm_ids)


if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
