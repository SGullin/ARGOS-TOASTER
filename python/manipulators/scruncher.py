import manipulators
import psrchive

plugin_name = 'scruncher'

def manipulate(infns, outname, nsub=1, nchan=1, nbin=None):
    """Scrunch the given archive in polarization, as well as
        in frequency to 'self.nchan' channels, and in time to 
        'self.nsub' subints. Also bin scrunch to 'self.nbin'.
        
        **Note: The Scruncher manipulation only works on
            one archive, so if the list 'infns' contains 
            more than one entry an exception is raised.

        Inputs:
            infns: A list of file names of input archvies. 
                NOTE: Only the first entry in this list is used.
            outname: File name of the manipulated archive.
            nsub: Number of output subints requested.
                (Default: 1)
            nchan: Number of output channels requested.
                (Default: 1)
            nbin: Number of output bins requested.
                (Default: Don't bin scrunch.)

        Outputs:
            None
    """
    # Ensure there is only a single input file
    if len(infns) != 1:
        raise manipulators.ManipulatorError("Scruncher manipulator " \
                    "accepts/requires only a single input file "\
                    "(%d provided)." % len(infns))

    # Load the input archives into python as Archive objects
    archives = manipulators.load_archives(infns)

    # Archives is a list of one (we ensure this above)
    scrunched = archives[0]

    # Scrunch the heck out of it
    scrunched.pscrunch()
    scrunched.fscrunch_to_nchan(nchan)
    scrunched.tscrunch_to_nsub(nsub)
    if nbin is not None:
        scrunched.bscrunch_to_nbin(nbin)

    # Unload the archive
    manipulators.unload_achive(scrunched, outname)   


def add_arguments(parser):
    """Add any arguments to subparser that are required 
        by the manipulator.

        Inputs:
            parser: An argparse subparser for which arguments should
                be added in-place.
        
        Outputs:
            None
    """
    parser.add_argument("--nsub", type=int, dest='nsub', default=1, \
                        help="Number of sub-ints to scrunch to. " \
                            "(Default: 1).")
    parser.add_argument("--nchan", type=int, dest='nchan', default=1, \
                        help="Number of sub-bands to scrunch to. " \
                            "(Default: 1).")
    parser.add_argument("--nbin", type=int, dest='nbin', default=None, \
                        help="Number of bins to scrunch to. " \
                            "(Default: don't bin scrunch).")
