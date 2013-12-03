"""Set the centre frequency of an archive, dedisperse and fully scrunch.
"""

import manipulators

plugin_name = 'ddfixfreq'

def manipulate(infns, outname, ctrfreq, dm=None):
    """Set centre frequency of the archive and dedisperse
        and scrunch fully.
    
        Input:
            infns: A list of file names of input archvies. 
                NOTE: Only the first entry in this list is used.
            outname: File name of the manipulated archive.
            ctrfreq: The centre frequency, in MHz, to use.
            dm: The DM to dedisperse to. (Default: use DM stored in archive header.)

        Outputs:
            None
    """
    # Ensure there is only a single input file
    if len(infns) != 1:
        raise manipulators.ManipulatorError("DD Fix Freq manipulator " \
                    "accepts/requires only a single input file "\
                    "(%d provided)." % len(infns))

    # Load the input archives into python as Archive objects
    archives = manipulators.load_archives(infns)

    # Archives is a list of one (we ensure this above)
    ar = archives[0]
    
    # Set the central frequency to use
    ar.set_centre_frequency(ctrfreq)

    # Optionally set the DM
    if dm is not None:
        ar.set_dispersion_measure(dm)

    # Scrunch and dedisperse
    ar.pscrunch()
    ar.tscrunch()
    ar.dedisperse()
    ar.fscrunch()

    # Unload the archive
    manipulators.unload_archive(ar, outname)   


def add_arguments(parser):
    """Add any arguments to subparser that are required 
        by the manipulator.

        Inputs:
            parser: An argparse subparser for which arguments should
                be added in-place.
        
        Outputs:
            None
    """
    parser.add_argument("--dm", type=float, dest='dm', \
                        help="DM to use when dedispersing. " \
                            "(Default: use DM in archive header).")
    parser.add_argument("--ctrfreq", type=float, dest='ctrfreq', \
                        required=True, \
                        help="Centre frequency (in MHz) to use for "
                            " when dedispersing. This is required.")
