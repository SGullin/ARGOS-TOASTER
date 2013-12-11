import sys
import os.path
import inspect

from toaster import config
from toaster import colour

def print_success(msg):
    """Print a success message.

        The message is colourized with the preset 'success' mode.

        Inputs:
            msg: The message to print.

        Outputs:
            None
    """
    colour.cprint(msg, 'success')
    sys.stdout.flush()

def print_info(msg, level=1):
    """Print an informative message if the current verbosity is
        higher than the 'level' of this message.

        The message will be colourized as 'info'.

        Inputs:
            msg: The message to print.
            level: The verbosity level of the message.
                (Default: 1 - i.e. don't print unless verbosity is on.)

        Outputs:
            None
    """
    if config.cfg.verbosity >= level:
        if config.cfg.excessive_verbosity:
            # Get caller info
            fn, lineno, funcnm = inspect.stack()[1][1:4]
            colour.cprint("INFO (level: %d) [%s:%d - %s(...)]:" % 
                    (level, os.path.split(fn)[-1], lineno, funcnm), 'infohdr')
            msg = msg.replace('\n', '\n    ')
            colour.cprint("    %s" % msg, 'info')
        else:
            colour.cprint(msg, 'info')
        sys.stdout.flush()


def print_debug(msg, category, stepsback=1):
    """Print a debugging message if the given debugging category
        is turned on.

        The message will be colourized as 'debug'.

        Inputs:
            msg: The message to print.
            category: The debugging category of the message.
            stepsback: The number of steps back into the call stack
                to get function calling information from. 
                (Default: 1).

        Outputs:
            None
    """
    if config.debug.is_on(category):
        if config.cfg.helpful_debugging:
            # Get caller info
            fn, lineno, funcnm = inspect.stack()[stepsback][1:4]
            to_print = colour.cstring("DEBUG %s [%s:%d - %s(...)]:\n" % \
                        (category.upper(), os.path.split(fn)[-1], lineno, funcnm), \
                            'debughdr')
            msg = msg.replace('\n', '\n    ')
            to_print += colour.cstring("    %s" % msg, 'debug')
        else:
            to_print = colour.cstring(msg, 'debug')
        sys.stderr.write(to_print + '\n')
        sys.stderr.flush()



