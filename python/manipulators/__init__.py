import inspect
import os.path
import shutil
import tempfile
import psrchive

registered_manipulators = ["scruncher", "ddfixfreq"]

auto_import_registered = True # If True, automatically import all registered
                              # manipulators when 'manipulators' is
                              # imported.

__all__ = registered_manipulators

plugin_name = NotImplemented

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
    archives = []
    for fn in fns:
        archives.append(psrchive.Archive_load(fn))


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


def run_manipulator(manipulator_func, infns, cmdopts, \
                        outname=None, tmpdir=None):
    """Set up a temporary directory to run the manipulator
        method in, load the archives, run the manipulator,
        get the result of the manipulator, break down the
        temporary directory and return the result of the
        manipulation.

        Inputs:
            manipulator_func: The manipulator function to use. 
            infns: Names of the input files to be passed to the
                manipulator
            cmdopts: argparse.Namespace object generated by
                parsing commandline options. The appropriate
                arguments are passed to the manipulator.
            outname: File name of the manipulated archive.
            tmpdir: Location of the temporary directory.
                (Default: let python's 'tempfile' module
                    put the temp dir in a standard location.)

        Outputs:
            None
    """
    workdir = tempfile.mkdtemp(dir=tmpdir, suffix='epta_toa_db')
    newfns = []
    for fn in infns:
        newfn = os.path.join(workdir, os.path.split(fn)[-1])
        shutil.copy(fn, newfn)
        newfns.append(newfn)
    try:
        print "Running %s" % cmdopts.manipulator
        print "With %s archives" % len(archives)
        print "And arguments %s" % cmdopts
        args = inspect.getargspec(manipulator_func)[0]
        kwargs = {}
        for arg in args:
            if arg != "archives":
                kwargs[arg] = getattr(cmdopts, arg)
        manipulator_func(newfns, outname, **kwargs)
    finally:
        shutil.rmtree(workdir)


class ManipulatorError(Exception):
    """A custom exception to be used by the manipulators.
        
        (This helps catch Manipulator-specific errors 
        that are understood, and expected.)
    """
    pass


if auto_import_registered:
    for manipulator in registered_manipulators:
        __import__(manipulator, globals())
