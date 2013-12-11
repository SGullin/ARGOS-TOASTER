"""Useful, general functions and data.
"""
import sys
import os
import shutil
import os.path
import datetime
import argparse
import hashlib
import subprocess
import types
import tempfile
import inspect
import warnings
import re

from toaster import config
from toaster import errors
from toaster import colour

##############################################################################
# GLOBAL DEFENITIONS
##############################################################################
# The following regular expressions are used when parse parfiles
float_re = re.compile(r"^[-+]?(\d+(\.\d*)?|\.\d+)([eEdD][-+]?\d+)?$")
int_re = re.compile(r"^[-+]?\d+$")


##############################################################################
# Functions
##############################################################################
def Give_UTC_now():
    utcnow = datetime.datetime.utcnow()
    return utcnow.strftime("%b %d, %Y - %H:%M:%S (UTC)")


def prep_parfile(fn):
    """Prepare parfile for archiving/loading.
        
        Also, perform some checks on the parfile to make sure we
        won't run into problems later. Checks peformed:
            - Existence of file.
            - Read/write access for file (so it can be moved).

        Input:
            fn: The name of the parfile to check.

        Outputs:
            params: A dictionary of parameters contained in the file.
                NOTE: parameters that look like ints or floats are cast
                    as such.
    """
    # Check existence of file
    Verify_file_path(fn)

    # Check file permissions allow for writing and reading
    if not os.access(fn, os.R_OK):
        raise errors.FileError("File (%s) is not readable!" % fn)

    # Grab parameters from file
    f = open(fn, 'r')
    params = {}
    for line in f.readlines():
        # Ignore blank lines
        line = line.strip()
        if not line:
            continue
        key, valstr = line.split()[:2]

        if int_re.match(valstr):
            # Looks like an int. Cast to int.
            val = int(valstr)
        elif float_re.match(valstr):
            # Looks like a float. Change 'D' to 'E' and cast to float.
            val = float(valstr.upper().replace('D','E'))
        else:
            # Doesn't seem like a number. Leave as string.
            val = valstr

        params[key.lower()] = val
    if "psrj" in params:
        params['pulsar_id'] = get_pulsarid(params['psrj'], \
                    autoadd=config.cfg.auto_add_pulsars)
        params['name'] = params['psrj']
    elif "psrb" in params:
        params['pulsar_id'] = get_pulsarid(params['psrb'], \
                    autoadd=config.cfg.auto_add_pulsars)
        params['name'] = params['psrb']
    else:
        params['pulsar_id'] = get_pulsarid(params['psr'], \
                    autoadd=config.cfg.auto_add_pulsars)
        params['name'] = params['psr']
    
    # Translate a few parameters
    if params.has_key('binary'):
        params['binary_model'] = params['binary']
    if params.has_key('e'):
        params['ecc'] = params['e']
    
    # Do some error checking
    if params.has_key('sini') and type(params['sini']) is types.StringType:
        # 'SINI' parameter can be 'KIN' in this case omit 'SINI' from
        # the database.
        params.pop('sini')

    # normalise pulsar name
    params['name'] = get_prefname(params['name'])
    params['user_id'] = get_userid()
    return params


def hash_password(pw):
    return hashlib.md5(pw).hexdigest()


def Get_md5sum(fname, block_size=16*8192):
    """Compute and return the MD5 sum for the given file.
        The file is read in blocks of 'block_size' bytes.

        Inputs:
            fname: The name of the file to get the md5 for.
            block_size: The number of bytes to read at a time.
                (Default: 16*8192)

        Output:
            md5: The hexidecimal string of the MD5 checksum.
    """
    f = open(fname, 'rb')
    md5 = hashlib.md5()
    block = f.read(block_size)
    while block:
        md5.update(block)
        block = f.read(block_size)
    f.close()
    return md5.hexdigest()


def make_proc_diagnostics_dir(fn, proc_id):
    """Given an archive, create the appropriate diagnostics
        directory, and cross-references.

        Inputs:
            fn: The file to create a diagnostic directory for.
            proc_id: The processing ID number to create a diagnostic
                directory for.
        
        Outputs:
            dir: The diagnostic directory's name.
    """
    diagnostics_location = os.path.join(config.cfg.data_archive_location, "diagnostics")
    params = prep_file(fn)
    basedir = get_archive_dir(fn, params=params, \
                    data_archive_location=diagnostics_location)
    dir = os.path.join(basedir, "procid_%d" % proc_id)
    # Make sure directory exists
    if not os.path.isdir(dir):
        # Create directory
        notify.print_info("Making diagnostic directory: %s" % dir, 2)
        os.makedirs(dir, 0770)

    crossrefdir = os.path.join(diagnostics_location, "processing")
    if not os.path.isdir(crossrefdir):
        # Create directory
        notify.print_info("Making diagnostic crossref dir: %s" % crossrefdir, 2)
        os.makedirs(crossrefdir, 0770)

    crossref = os.path.join(crossrefdir, "procid_%d" % proc_id)
    if not os.path.islink(crossref):
        # Create symlink
        notify.print_info("Making crossref to diagnostic dir: %s" % crossref, 2)
        os.symlink(dir, crossref)
    
    return dir


def execute(cmd, stdout=subprocess.PIPE, stderr=sys.stderr, \
                dir=None, stdinstr=None):
    """Execute the command 'cmd' after logging the command
        to STDOUT. Execute the command in the directory 'dir',
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
    if dir is not None:
        msg = "(In %s)\n%s" % (dir, str(cmd))
    else:
        msg = str(cmd)
    notify.print_debug(msg, "syscalls", stepsback=2)

    stdoutfile = False
    stderrfile = False
    if type(stdout) == types.StringType:
        stdout = open(stdout, 'w')
        stdoutfile = True
    if type(stderr) == types.StringType:
        stderr = open(stderr, 'w')
        stderrfile = True

    if stdinstr is not None:
        notify.print_debug("Sending the following to cmd's stdin: %s" % stdinstr, \
                        "syscalls")
        # Run (and time) the command. Check for errors.
        pipe = subprocess.Popen(cmd, shell=False, cwd=dir, \
                            stdin=subprocess.PIPE, 
                            stdout=stdout, stderr=stderr)
        (stdoutdata, stderrdata) = pipe.communicate(stdinstr)
    else:
        # Run (and time) the command. Check for errors.
        pipe = subprocess.Popen(cmd, shell=False, cwd=dir , \
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

    return (stdoutdata, stderrdata)


def parse_pat_output(patout):
    """Parse the output from 'pat'.
        
        Input:
            patout: The stdout output of running 'pat'.

        Output:
            toainfo: A list of dictionaries, each with
                information for a TOA.
    """
    toainfo = []
    for toastr in patout.split("\n"):
        toastr = toastr.strip()
        if toastr and (toastr != "FORMAT 1") and \
                    (not toastr.startswith("Plotting")):
            toasplit = toastr.split()
            freq = float(toasplit[1])
            imjd = float(toasplit[2].split(".")[0])
            fmjd = float("0." + toasplit[2].split(".")[1])
            err = float(toasplit[3])
            if '-gof' in toasplit:
                # The goodness-of-fit is only calculated for the 'FDM'
                # fitting method. The GoF value returned for other 
                # methods is innaccurate.
                gofvalstr = toasplit[toasplit.index('-gof')+1]
                if config.cfg.toa_fitting_method=='FDM' and gofvalstr!='*error*':
                    gof = float(gofvalstr)
                else:
                    gof = None
            if ('-bw' in toasplit) and ('-nchan' in toasplit):
                nchan = int(toasplit[toasplit.index('-nchan')+1])
                bw = float(toasplit[toasplit.index('-bw')+1])
                bw_per_toa = bw/nchan
            else:
                bw_per_toa = None
            if ('-length' in toasplit) and ('-nsubint' in toasplit):
                nsubint = int(toasplit[toasplit.index('-nsubint')+1])
                length = float(toasplit[toasplit.index('-length')+1])
                length_per_toa = length/nsubint
            else:
                length_per_toa = None
            if ('-nbin' in toasplit):
                nbin = int(toasplit[toasplit.index('-nbin')+1])
            toainfo.append({'freq':freq, \
                            'imjd':imjd, \
                            'fmjd':fmjd, \
                            'toa_unc_us':err, \
                            'goodness_of_fit':gof, \
                            'bw':bw_per_toa, \
                            'length':length_per_toa, \
                            'nbin':nbin})
    return toainfo


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
        group = self.add_argument_group("Standard Options", \
                    "The following options get used by various programs.")
        group.add_argument('-v', '--more-verbose', nargs=0, \
                            action=self.TurnUpVerbosity, \
                            help="Be more verbose. (Default: " \
                                 "verbosity level = %d)." % config.cfg.verbosity)
        group.add_argument('-q', '--less-verbose', nargs=0, \
                            action=self.TurnDownVerbosity, \
                            help="Be less verbose. (Default: " \
                                 "verbosity level = %d)." % config.cfg.verbosity)
        group.add_argument('--set-verbosity', nargs=1, dest='level', \
                            action=self.SetVerbosity, type=int, \
                            help="Set verbosity level. (Default: " \
                                 "verbosity level = %d)." % config.cfg.verbosity)
        group.add_argument('-W', '--warning-mode', dest='warnmode', type=str, \
                            help="Set a filter that applies to all warnings. " \
                                "The behaviour of the filter is determined " \
                                "by the action provided. 'error' turns " \
                                "warnings into errors, 'ignore' causes " \
                                "warnings to be not printed. 'always' " \
                                "ensures all warnings are printed. " \
                                "(Default: print the first occurence of " \
                                "each warning.)")
        group.add_argument('--config-file', dest='cfg_file', \
                            action=self.LoadConfigFile, type=str, \
                            help="Configuration file to load. (Default: " \
                                "no personalized configs are loaded.)")
        self.added_std_group = True

    def add_debug_group(self):
        if self.added_debug_group:
            # Debug group has already been added
            return
        group = self.add_argument_group("Debug Options", \
                    "The following options turn on various debugging " \
                    "statements. Multiple debugging options can be " \
                    "provided.")
        group.add_argument('-d', '--debug', nargs=0, \
                            action=self.SetAllDebugModes, \
                            help="Turn on all debugging modes. (Same as --debug-all).")
        group.add_argument('--debug-all', nargs=0, \
                            action=self.SetAllDebugModes, \
                            help="Turn on all debugging modes. (Same as -d/--debug).")
        group.add_argument('--set-debug-mode', nargs=1, dest='mode', \
                            action=self.SetDebugMode, \
                            help="Turn on specified debugging mode. Use " \
                                "--list-debug-modes to see the list of " \
                                "available modes and descriptions. " \
                                "(Default: all debugging modes are off)")
        group.add_argument('--list-debug-modes', nargs=0, \
                            action=self.ListDebugModes, \
                            help="List available debugging modes and " \
                                "descriptions, then exit")
        self.added_debug_group = True

    class LoadConfigFile(argparse.Action):
        def __call__(self, parser, namespace, values, option_string):
            config.cfg.load_configs(values)

    class TurnUpVerbosity(argparse.Action):
        def __call__(self, parser, namespace, values, option_string):
            config.cfg.verbosity += 1

    class TurnDownVerbosity(argparse.Action):
        def __call__(self, parser, namespace, values, option_string):
            config.cfg.verbosity -= 1
 
    class SetVerbosity(argparse.Action):
        def __call__(self, parser, namespace, values, option_string):
            config.cfg.verbosity = values[0]

    class SetDebugMode(argparse.Action): 
        def __call__(self, parser, namespace, values, option_string):
            config.debug.set_mode_on(values[0])

    class SetAllDebugModes(argparse.Action): 
        def __call__(self, parser, namespace, values, option_string):
            config.debug.set_allmodes_on()

    class ListDebugModes(argparse.Action): 
        def __call__(self, parser, namespace, values, option_string):
            print "Available debugging modes:"
            for name, desc in config.debug.modes:
                if desc is None:
                    continue
                print "    %s: %s" % (name, desc)
            sys.exit(1)


