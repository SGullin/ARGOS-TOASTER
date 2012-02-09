"""
This module maintains a list of debug options for the EPTA 
timing pipline, and their current settings. This way the listing 
of debug modes currently enabled can be shared between different 
modules of a single process.

Patrick Lazarus, Feb. 9, 2012
"""

modes = [('syscalls', 'Print commands being executed as system calls.'), \
            ]

modes.sort()

# By default set all debug modes to False
for ii, (m, desc) in enumerate(modes):
    exec("%s = False" % m.upper())


def set_mode_on(*modes):
    for m in modes:
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
                        
    
def print_debug_status():
    on_modes = get_on_modes()
    print "The following debugging modes are turned on:"
    for m in on_modes:      
        print "    %s" % m  
                          
