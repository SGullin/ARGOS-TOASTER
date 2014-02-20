"""Useful, general functions and data.
"""
import sys
import datetime
import argparse
import hashlib
import subprocess
import types
import warnings
import re

from toaster import config
from toaster import errors
from toaster import debug
from toaster.utils import notify


##############################################################################
# GLOBAL DEFINITIONS
##############################################################################
# The following regular expressions are used when parse parfiles

float_re = re.compile(r"^[-+]?(\d+(\.\d*)?|\.\d+)([eEdD][-+]?\d+)?$")
int_re = re.compile(r"^[-+]?\d+$")


##############################################################################
# Functions
##############################################################################
def give_utc_now():
    utcnow = datetime.datetime.utcnow()
    return utcnow.strftime("%b %d, %Y - %H:%M:%S (UTC)")


def hash_password(pw):
    return hashlib.md5(pw).hexdigest()


def execute(cmd, stdout=subprocess.PIPE, stderr=sys.stderr,
            execdir=None, stdinstr=None):
    """Execute the command 'cmd' after logging the command
        to STDOUT. Execute the command in the directory 'execdir',
        which defaults to the current directory is not provided.

        Output standard output to 'stdout' and standard
        error to 'stderr'. Both are strings containing filenames.
        If values are None, the out/err streams are not recorded.
        By default stdout is subprocess.PIPE and stderr is sent 
        to sys.stderr.

        If stdinstr is not None, send the string to the command as
        data in the stdin stream.

        Returns (stdoutdata, stderrdata). These will both be None, 
        unless subprocess.PIPE is provided.
    """
    # Log command to stdout
    if execdir is not None:
        msg = "(In %s)\n%s" % (execdir, str(cmd))
    else:
        msg = str(cmd)
    notify.print_debug(msg, "syscalls", stepsback=2)

    stdoutfile = False
    stderrfile = False
    if isinstance(stdout, str):
        stdout = open(stdout, 'w')
        stdoutfile = True
    if isinstance(stderr, str):
        stderr = open(stderr, 'w')
        stderrfile = True

    if stdinstr is not None:
        notify.print_debug("Sending the following to cmd's stdin: %s" % stdinstr, \
                           "syscalls")
        # Run (and time) the command. Check for errors.
        pipe = subprocess.Popen(cmd, shell=False, cwd=execdir,
                                stdin=subprocess.PIPE,
                                stdout=stdout, stderr=stderr)
        (stdoutdata, stderrdata) = pipe.communicate(stdinstr)
    else:
        # Run (and time) the command. Check for errors.
        pipe = subprocess.Popen(cmd, shell=False, cwd=execdir,
                                stdout=stdout)#, stderr=stderr)
        (stdoutdata, stderrdata) = pipe.communicate()
    retcode = pipe.returncode
    if retcode < 0:
        raise errors.SystemCallError("Execution of command (%s) terminated by signal (%s)!" % \
                                     (cmd, -retcode))
    elif retcode > 0:
        raise errors.SystemCallError("Execution of command (%s) failed with status (%s)!" % \
                                     (cmd, retcode))
    else:
        # Exit code is 0, which is "Success". Do nothing.
        pass

    # Close file objects, if any
    if stdoutfile:
        stdout.close()
    if stderrfile:
        stderr.close()

    return stdoutdata, stderrdata


def set_warning_mode(mode=None, reset=True):
    """Add a simple warning filter.
        
        Inputs:
            mode: The action to use for warnings.
                (Default: take value of 'warnmode' configuration.
            reset: Remove warning filters previously set.

        Outputs:
            None
    """
    if mode is None:
        mode = config.cfg.warnmode
    if reset:
        warnings.resetwarnings()
    warnings.simplefilter(mode)


class DefaultArguments(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        self.added_std_group = False
        self.added_debug_group = False
        argparse.ArgumentParser.__init__(self, *args, **kwargs)

    def parse_args(self, *args, **kwargs):
        if not self._subparsers:
            # Add default groups just before parsing so it is the last set of
            # options displayed in help text
            self.add_standard_group()
            self.add_debug_group()
        args = argparse.ArgumentParser.parse_args(self, *args, **kwargs)
        if not self._subparsers:
            set_warning_mode(args.warnmode)
        return args

    def parse_known_args(self, *args, **kwargs):
        if not self._subparsers:
            # Add default groups just before parsing so it is the last set of
            # options displayed in help text
            self.add_standard_group()
            self.add_debug_group()
        args, leftovers = argparse.ArgumentParser.parse_known_args(self, *args, **kwargs)
        if not self._subparsers:
            set_warning_mode(args.warnmode)
        return args, leftovers

    def add_standard_group(self):
        if self.added_std_group:
            # Already added standard group
            return
        group = self.add_argument_group("Standard Options",
                                        "The following options get used by various programs.")
        group.add_argument('-v', '--more-verbose', nargs=0,
                           action=self.TurnUpVerbosity,
                           help="Be more verbose. (Default: "
                                "verbosity level = %d)." % config.cfg.verbosity)
        group.add_argument('-q', '--less-verbose', nargs=0,
                           action=self.TurnDownVerbosity,
                           help="Be less verbose. (Default: "
                                "verbosity level = %d)." % config.cfg.verbosity)
        group.add_argument('--set-verbosity', nargs=1, dest='level',
                           action=self.SetVerbosity, type=int,
                           help="Set verbosity level. (Default: "
                                "verbosity level = %d)." % config.cfg.verbosity)
        group.add_argument('-W', '--warning-mode', dest='warnmode', type=str,
                           help="Set a filter that applies to all warnings. "
                                "The behaviour of the filter is determined "
                                "by the action provided. 'error' turns "
                                "warnings into errors, 'ignore' causes "
                                "warnings to be not printed. 'always' "
                                "ensures all warnings are printed. "
                                "(Default: print the first occurrence of "
                                "each warning.)")
        group.add_argument('--config-file', dest='cfg_file',
                           action=self.LoadConfigFile, type=str,
                           help="Configuration file to load. (Default: "
                                "no personalized configs are loaded.)")
        self.added_std_group = True

    def add_debug_group(self):
        if self.added_debug_group:
            # Debug group has already been added
            return
        group = self.add_argument_group("Debug Options",
                                        "The following options turn on various debugging "
                                        "statements. Multiple debugging options can be "
                                        "provided.")
        group.add_argument('-d', '--debug', nargs=0,
                           action=self.SetAllDebugModes,
                           help="Turn on all debugging modes. (Same as --debug-all).")
        group.add_argument('--debug-all', nargs=0,
                           action=self.SetAllDebugModes,
                           help="Turn on all debugging modes. (Same as -d/--debug).")
        group.add_argument('--set-debug-mode', dest='mode',
                           action=self.SetDebugMode,
                           help="Turn on specified debugging mode. Use "
                                "--list-debug-modes to see the list of "
                                "available modes and descriptions. "
                                "(Default: all debugging modes are off)")
        group.add_argument('--list-debug-modes', nargs=0,
                           action=self.ListDebugModes,
                           help="List available debugging modes and "
                                "descriptions, then exit")
        self.added_debug_group = True

    class LoadConfigFile(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            config.cfg.load_configs(values)

    class TurnUpVerbosity(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            config.cfg.verbosity += 1

    class TurnDownVerbosity(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            config.cfg.verbosity -= 1

    class SetVerbosity(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            config.cfg.verbosity = values[0]

    class SetDebugMode(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            debug.set_mode_on(values)

    class SetAllDebugModes(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            debug.set_allmodes_on()

    class ListDebugModes(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            print "Available debugging modes:"
            for name, desc in debug.get_modes_and_descriptions():
                if desc is None:
                    continue
                print "    %s: %s" % (name, desc)
            sys.exit(1)


def sort_by_keys(tosort, keys):
    """Sort a list of dictionaries, or database rows
        by the list of keys provided. Keys provided
        later in the list take precedence over earlier
        ones. If a key ends in '_r' sorting by that key
        will happen in reverse.

        Inputs:
            tosort: The list to sort.
            keys: The keys to use for sorting.

        Outputs:
            None - sorting is done in-place.
    """
    if not tosort:
        return tosort
    notify.print_info("Sorting by keys (%s)" % " then ".join(keys), 3)
    for sortkey in keys:
        if sortkey.endswith("_r"):
            sortkey = sortkey[:-2]
            rev = True
            notify.print_info("Reverse sorting by %s..." % sortkey, 2)
        else:
            rev = False
            notify.print_info("Sorting by %s..." % sortkey, 2)
        if type(tosort[0][sortkey]) is types.StringType:
            tosort.sort(key=lambda x: x[sortkey].lower(), reverse=rev)
        else:
            tosort.sort(key=lambda x: x[sortkey], reverse=rev)