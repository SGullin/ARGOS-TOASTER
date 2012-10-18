"""
This module maintains a list of debug options for the EPTA 
timing pipline, and their current settings. This way the listing 
of debug modes currently enabled can be shared between different 
modules of a single process.

Patrick Lazarus, Feb. 9, 2012
"""

import errors

modes = [('syscalls', 'Print commands being executed as system calls.'), \
         ('queries', 'Print database queries being executed.'), \
         ('manipulator', "Print debugging info for manipulators."), \
         ('gittest', "Raise warnings instead of errors when checking " \
                        "git repos. This is useful for testing " \
                        "un-committed changes."), \
            ]

modes.sort()

mode_names = []
# By default set all debug modes to False
for ii, (m, desc) in enumerate(modes):
    mode_names.append(m)
    exec("%s = False" % m.upper())


def set_mode_on(*modes_toset):
    for m in modes_toset:
        if m not in mode_names:
            raise errors.BadDebugMode("The debug mode '%s' doesn't exist!" % m)
        exec "%s = True" % m.upper() in globals()


def set_allmodes_on():
    for m, desc in modes:
        exec "%s = True" % m.upper() in globals()


def set_allmodes_off():
    for m, desc in modes: 
        exec "%s = False" % m.upper() in globals()


def set_mode_off(*modes):
    for m in modes:
        exec "%s = False" % m.upper() in globals()


def get_on_modes():     
    on_modes = []                   
    for m, desc in modes:           
        if eval('%s' % m.upper()):
            on_modes.append('debug.%s' % m.upper())
    return on_modes         


def is_on(mode):
    return eval('%s' % mode.upper())

    
def print_debug_status():
    on_modes = get_on_modes()
    print "The following debugging modes are turned on:"
    for m in on_modes:      
        print "    %s" % m  
                          
