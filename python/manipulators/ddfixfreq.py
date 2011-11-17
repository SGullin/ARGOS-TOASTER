import manipulators
import psrchive

plugin_name = 'ddfixfreq'

def manipulate(archives, ctrfreq, dm=None):
    """Set centre frequency of the archive and dedisperse
        and scrunch fully.
    
        Input:
            ctrfreq: The centre frequency, in MHz, to use.
            dm: The DM to dedisperse to. (Default: use DM stored in archive header.)

        Outputs:
            None

    """
    ar = archives[0].clone()
    ar.set_centre_frequency(ctrfreq)
    if dm is not None:
        ar.set_dispersion_measure(dm)
    ar.pscrunch()
    ar.tscrunch()
    ar.dedisperse()
    ar.fscrunch()
    return ar

def add_arguments(parser):
    """Add any arguments to subparser that are required 
        by the manipulator.

        Inputs:
            parser: a argparse subparser for which arguments should
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
