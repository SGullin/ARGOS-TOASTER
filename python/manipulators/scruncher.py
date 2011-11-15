import psrchive

class Scruncher(manipulators.Manipulator):
    """A class for scrunching a single archive.
    """
    def __init__(self, archive_fns):
        """Constructor for Scruncher objects.
            Calls the super class' constructor.

            Inputs:
                archive_fns: list of archive filenames.

            Outputs:
                manipulator object

            **Note: The Scruncher manipulation only works on
                one archive, so if the list 'archive_fns' 
                contains more than one entry an exception is
                raised
        """
        if len(archive_fns)>1:
            raise ValueError("Only one archive should be input to " \
                             "the Scruncher manipulator.")
        super(Schruncher, self).__init__(archive_fns)

    def manipulate(self, nsub=1, nchan=1, nbin=None):
        """Scrunch the given archive in polarization, as well as
            in frequency to 'nchan' channels, and in time to 'nsub'
            subints. Also bin scrunch to 'nbin'.

            Inputs:
                nsub: Number of output subints requested.
                    (Default: 1)
                nchan: Number of output channels requested.
                    (Default: 1)
                nbin: Number of output bins requested.
                    (Default: Don't bin scrunch.)

            Outputs:
                scrunched: The scrunched archive.
        """
        scrunched = copy.deepcopy(self.archives[0])
        scrunched.pscrunch()
        scrunched.fscrunch_to_nchan(nchan)
        scrunched.tscrunch_to_nsub(nsub)
        if nbin is not None:
            scrunched.bscrunch_to_nbin(nbin)
        return scrunched

