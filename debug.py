"""
This module maintains a list of debug options for TOASTER, 
and their current settings. This way the listing of debug 
modes currently enabled can be shared between different 
modules of a single process.

Patrick Lazarus, Feb. 9, 2012
"""

from toaster import errors

MODE_DEFS = {'syscalls': 'Print commands being executed as system calls.',
             'queries': 'Print database queries being executed.',
             'manipulator': "Print debugging info for manipulators.",
             'gittest': "Raise warnings instead of errors when checking "
                        "git repos. This is useful for testing "
                        "un-committed changes.",
             'config': "Display what config files are loaded.",
             'database': "Display DB connection/transaction info.",
             'toaparse': "Display info when parsing TOAs from timfiles.",
             'timfile': None}

ONMODES = {}


def set_mode_on(*modes_toset):
    for mode in modes_toset:
        if mode not in MODE_DEFS:
            raise errors.BadDebugMode("The debug mode '%s' doesn't exist!" % mode)
        ONMODES[mode] = True

def set_allmodes_on():
    for mode in MODE_DEFS:
        ONMODES[mode] = True


def set_allmodes_off():
    for mode in MODE_DEFS:
        ONMODES[mode] = False


def set_mode_off(*modes):
    for mode in modes:
        ONMODES[mode] = False


def get_on_modes():
    on_modes = []
    for mode, is_it_on in ONMODES.iteritems():
        if is_it_on:
            on_modes.append(mode)
    return on_modes         


def is_on(mode):
    return ONMODES[mode]

    
def print_debug_status():
    on_modes = get_on_modes()
    print "The following debugging modes are turned on:"
    if on_modes:
        for mode in on_modes:
            print "    %s" % mode
    else:
        print "    None"


# By default set all debug modes to False
set_allmodes_off()

