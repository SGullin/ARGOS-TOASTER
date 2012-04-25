"""Use pam to scrunch an archive and (optionally) reinstall an
    ephmemeris.
"""

import shutil

import manipulators
import epta_pipeline_utils as epu

plugin_name = 'pamit'

def manipulate(infns, outname, nsub=1, nchan=1, nbin=None, \
                ephem=None, update_dm=True):
    """Scrunch the given archive in polarization, as well as
        in frequency to 'nchan' channels, and in time to 
        'nsub' subints. Also bin scrunch to 'nbin'.
        Optionally, a new ephemeris will be installed and the DM will
        be updated accordingly.
        
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
            ephem: The filename of the ephemeris to install.
                (Default: Don't install a new ephemeris.)
            update_dm: If True, update the archive's DM value in
                its header to be the same as what is in the newly
                installed ephemeris. (Default: True).

        Outputs:
            None
    """
    # Ensure there is only a single input file
    if len(infns) != 1:
        raise manipulators.ManipulatorError("Pam-it manipulator " \
                    "accepts/requires only a single input file "\
                    "(%d provided)." % len(infns))

    # Copy input archive to outname and modify that file in place
    # infns is a list of one (we ensure this above)
    shutil.copy(infns[0], outname)

    cmd = "pam -m --setnchn %d --setnsub %d %s" % \
            (nchan, nsub, outname)

    if ephem is not None:
        cmd += " -E %s" % ephem
        if update_dm:
            cmd += " --update-dm"

    if nbin is not None:
        cmd += " --setnbin %d" % nbin

    # Scrunch the heck out of it
    epu.execute(cmd)


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
