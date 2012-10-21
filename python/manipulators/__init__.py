import sys
import inspect
import os.path
import shutil
import tempfile
import argparse
import textwrap

import colour
import errors
import epta_pipeline_utils as epu

registered_manipulators = ["pamit"]

__all__ = registered_manipulators

class BaseManipulator(object):
    """The base class of Manipulator objects.
        
        Manipulator objects take an input list of archives,
        manipulate them, and return a single output archive.
    """
    name = NotImplemented
    def __init__(self):
        self.parser = argparse.ArgumentParser(add_help=False, \
                                    usage=argparse.SUPPRESS, \
                                    description="The '%s' manipulator -- %s" % \
                                                (self.name, self.description))
        self._add_arguments(self.parser)
        # Grab the default arguments directly, in case the user
        # Doesn't parse any command line arguments.
        #self.default_kwargs = {}
        #for action in self.parser._actions:
        #    if action.default is not None:
        #        self.default_kwargs[action.dest] = action.default

    def parse_args(self, args):
        tmp = self.parser.parse_args(args)
        self.kwargs = dict(tmp._get_kwargs())

    def _manipulate(self, infns, outname):
        raise NotImplementedError("The '_manipulate' method of Manipulator "
                                    "classes must be defined.")


    def _add_arguments(self, parser):
        """Given an argparse.ArgumentParser instance add 
            command line argument specific to this manipulator.

            NOTE: Each of the arguments to the manipulator's
                manipulate method should have an argument here.
            ALSO: Arguments should not be required. They should
                provide a default instead.

            Input:
                parser: The argparse.ArgumentParser instance
                    to add arguments to.

            Outputs:
                None
        """
        pass

    def run(self, infns, outname, tmpdir=None, **override_kwargs):
        """Set up a temporary directory to run the manipulator
            method in, load the archives, run the manipulator,
            get the result of the manipulator, break down the
            temporary directory and return the result of the
            manipulation.
 
            Inputs:
                prepped_manipfunc: A prepared manipulator function to use. 
                infns: Names of the input files to be passed to the
                    manipulator
                outname: File name of the manipulated archive.
                tmpdir: Location of the temporary directory.
                    (Default: let python's 'tempfile' module
                        put the temp dir in a standard location.)
                ** Other key-word arguments are used to override 
                    default arguments parsed by the manipulator's
                    parser.
 
            Outputs:
                None
        """
        manip_kwargs = {}
        #manip_kwargs.update(self.default_kwargs)
        manip_kwargs.update(self.kwargs)
        manip_kwargs.update(**override_kwargs)

        workdir = tempfile.mkdtemp(dir=tmpdir, suffix='toaster')
        newfns = []
        for fn in infns:
            newfn = os.path.join(workdir, os.path.split(fn)[-1])
            shutil.copy(fn, newfn)
            newfns.append(newfn)
        try:
            self._manipulate(newfns, outname, **manip_kwargs)
        finally:
            shutil.rmtree(workdir)


class ManipulatorArguments(epu.DefaultArguments):
    def __init__(self, *args, **kwargs):
        super(ManipulatorArguments, self).__init__(add_help=False, \
                                                *args, **kwargs)
        self.add_argument('-m', '--manipulator', dest='manip_name', \
                            default=registered_manipulators[0], \
                            choices=registered_manipulators, \
                            help="The name of the manipulator plugin to use")
        self.add_argument('--list-manipulators', nargs=0, \
                            action=self.ListManipulatorsAction, \
                            help="List available manipulators and " \
                                "descriptions, them exit.")
        self.add_argument('-h', '--help', nargs='?', dest='help_topic', \
                            metavar='MANIPULATOR', \
                            action=self.HelpAction, type=str, \
                            help="Display this help message. If provided "
                                "with the name of a manipulator, display "
                                "its help.")

    class HelpAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string):
            if values is None:
                parser.print_help()
            else:
                manip = load_manipulator(values)
                manip.parser.print_help()
            sys.exit(1)

    class ListManipulatorsAction(argparse.Action):
        def __call__(self, parser, namespace, values, option_string):
            for name in sorted(registered_manipulators):
                manip = load_manipulator(name)
                colour.cprint("Available Manipulators:", \
                                bold=True, underline=True) 
                wrapper = textwrap.TextWrapper(subsequent_indent=" "*(len(name)+4))
                print "%s -- %s" % (colour.cstring(name, bold=True), 
                                        wrapper.fill(manip.description))
            sys.exit(1)
        

def load_manipulator(manip_name):
    """Import a manipulator class and return an instance.
        
        Input:
            manip_name: The name of the manipulator.

        Output:
            manip: A manipulator instance.
    """
    if manip_name not in registered_manipulators:
        raise errors.UnrecognizedValueError("The manipulator, '%s', " \
                    "is not a registered manipulator. The following " \
                    "are registered: '%s'" % \
                    (manip_name, "', '".join(registered_manipulators)))
    mod = __import__(manip_name, globals())
    return mod.Manipulator()


def load_archives(fns):
    """A utility function to be used by manipulators.
        
        Given a list of archive file names load them using
        the 'psrchive' module and return a list of Archive 
        objects.

        Input:
            fns: A list of archive file names.

        Output:
            archives: A list of Archive objects.
    """
    import psrchive # Temporarily move import here in case 
                    # psrchive bindings aren't installed
    archives = []
    for fn in fns:
        archives.append(psrchive.Archive_load(fn))
    return archives


def unload_archive(archive, outname):
    """A utility function to be used by manipulators.
        
        Given an Archive object unload it to the file given
        by 'outname'. Note that this function checks to make
        sure no file will get clobbered by the unloading.

        Inputs:
            archive: The Archive object to unload.
            outname: The file name to unload to. There must be
                no file already existing with this name.

        Outputs:
            None
    """
    if not os.path.exists(outname):
        archive.unload(outname)
    else:
        raise ManipulatorError("Will not unload archive to '%s'. " \
                                "A file by that name already exists" % outname)


# Is the stuff below still needed??

def run_manipulator(prepped_manipfunc, infns, outname=None, \
                    tmpdir=None):
    """Set up a temporary directory to run the manipulator
        method in, load the archives, run the manipulator,
        get the result of the manipulator, break down the
        temporary directory and return the result of the
        manipulation.

        Inputs:
            prepped_manipfunc: A prepared manipulator function to use. 
            infns: Names of the input files to be passed to the
                manipulator
            outname: File name of the manipulated archive.
            tmpdir: Location of the temporary directory.
                (Default: let python's 'tempfile' module
                    put the temp dir in a standard location.)

        Outputs:
            None
    """
    workdir = tempfile.mkdtemp(dir=tmpdir, prefix='toaster_tmp', \
                                            suffix='_workdir')
    newfns = []
    for fn in infns:
        newfn = os.path.join(workdir, os.path.split(fn)[-1])
        shutil.copy(fn, newfn)
        newfns.append(newfn)
    try:
        prepped_manipfunc(newfns, outname)
    finally:
        if debug.MANIPULATOR:
            epu.print_debug("Manipulator worked in %s. Not removing it.", \
                                'manipulator')
        else:
            shutil.rmtree(workdir)


def extract_manipulator_arguments(manipulator_func, cmdopts):
    args = inspect.getargspec(manipulator_func)[0]
    kwargs = {}
    for arg in args:
        # Don't pass along arguments that we include explicitly
        if arg not in ("infns", "outname"):
            if hasattr(cmdopts, arg):
                # Assume arguments that are not provided have default values
                kwargs[arg] = getattr(cmdopts, arg)
    return kwargs 


def prepare_manipulator(manipulator_func, kwargs):
    """Given a dictionary of keyword arguments prepare the
        manipulator function, and return a lambda function
        that only requires two arguments:
            1. a list of input file names, and 
            2. an output file name

        Inputs:
            manipulator_func: The manipulator function.
            kwargs: A dictionary of keyword arguments to provide
                to the manipulator function.

        Output:
            prepped_func: A prepared version of the manipulator function.
    """
    return lambda infns, outfn: manipulator_func(infns, outfn, **kwargs)


class ManipulatorError(Exception):
    """A custom exception to be used by the manipulators.
        
        (This helps catch Manipulator-specific errors 
        that are understood, and expected.)
    """
    pass


