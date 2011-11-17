import os.path
import shutil
import tempfile
import psrchive

registered_manipulators = ["scruncher"]

auto_import_registered = True # If True, automatically import all registered
                              # manipulators when 'manipulators' is
                              # imported.

__all__ = registered_manipulators

plugin_name = NotImplemented

def run_manipulator(manipulator_func, infns, tmpdir=None, **kwargs):
    """Set up a temporary directory to run the manipulator
        method in, load the archives, run the manipulator,
        get the result of the manipulator, break down the
        temporary directory and return the result of the
        manipulation.

        Inputs:
            manipulator_func: The manipulator function to use. 
            infns: Names of the input files to be passed to the
                manipulator
            tmpdir: Location of the temporary directory.
                (Default: let python's 'tempfile' module
                    put the temp dir in a standard location.)
            **NOTE: Other keyword arguments are passed to the 
                manipulator function.

        Outputs:
            manipulated: A psrchive.Archive object that is
                the manipulated archive. This archive should
                be written out to disk my the user.
    """
    workdir = tempfile.mkdtemp(dir=tmpdir, suffix='epta_toa_db')
    archives = []
    for fn in infns:
        newfn = os.path.join(workdir, os.path.split(fn)[-1])
        shutil.copy(fn, newfn)
        archives.append(psrchive.Archive_load(newfn))
    try:
        print "Running %s" % manipulator
        print "With %s archives" % archives
        print "And arguments %s" % kwargs
        manipulated = manipulator_func(archives, **kwargs)
    finally:
        shutil.rmtree(workdir)
    return manipulated


if auto_import_registered:
    for manipulator in registered_manipulators:
        __import__(manipulator, globals())
