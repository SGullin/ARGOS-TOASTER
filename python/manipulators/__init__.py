import shutil
import tempfile
import psrchive

registered_manipulator_classes = ["scruncher"]

auto_import_registered = True # If True, automatically import all registered
                              # manipulator classes when 'manipulators' is
                              # imported.

__all__ = registered_manipulator_classes

class Manipulator(object):
    """A generic class to manipulate one or more PSRCHIVE archives.
        This object must implement a 'manipulate' method that
        takes a list of archive objects and (possibly) other arguments
        and returns an output archive object.
    """
    def __init__(self, archive_fns):
        """Constructor for Manipulator objects.
            Creates a temporary directory and copies archives
            there. This is where the files will be worked on.

            Inputs:
                archive_fns: list of archive filenames.

            Outputs:
                manipulator object
        """
        self.tempdir = tempfile.mkdtemp(suffix='epta_toa_db')
        self.archives = []
        self.archive_fns = []
        for fn in archive_fns:
            newfn = os.path.join(self.tempdir, os.path.split(fn)[0])
            shutil.copy(fn, newfn)
            self.archive_fns.append(newfn)
            self.archives.append(psrchive.Archive_load(newfn))

    def __del__(self):
        """Destructor for Manipulator onjects.
            Deletes temporary directory where working version
            of archives were placed.
            
            Inputs:
                None

            Outputs:
                None
        """
        for ar in self.archives:
            ar.__del__()
            del ar
        shutil.rmtree(self.tempdir)

    def manipulate(self):
        """Manipulate the archives.
        """
        raise NotImplementedError("The 'manipulate(...)' method of %s " \
                                "hasn't been implemented!" % self.__name__)

# This code needs to be at the bottom of the script because
# the imported subclasses require the classes, variables above
# to be defined.
if auto_import_registered:
    for classname in registered_manipulator_classes:
        __import__(classname, globals())
