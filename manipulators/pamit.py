"""Use pam to scrunch an archive and (optionally) reinstall an
    ephmemeris.
"""
import shutil
import argparse

import manipulators
import utils


class PamitManipulator(manipulators.BaseManipulator):
    name = 'pamit'
    description = "Use pam to scrunch the archive."

    def _manipulate(self, infns, outname, nsub=1, nchan=1,
                    nbin=None, tsub=None):
        """Scrunch the given archive in polarization, as well as
            in frequency to 'nchan' channels, and in time to 
            'nsub' subints. Also bin scrunch to 'nbin'.
            
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
                tsub: Number of seconds to include in each subint.
                    This overrides 'nsub'. (Default: use 'nsub')

            Outputs:
                None
        """
        # Ensure there is only a single input file
        if len(infns) != 1:
            raise manipulators.ManipulatorError("Pam-it manipulator "
                                                "accepts/requires only a "
                                                "single input file "
                                                "(%d provided)." % len(infns))

        # Copy input archive to outname and modify that file in place
        # infns is a list of one (we ensure this above)
        shutil.copy(infns[0], outname)

        cmd = ["pam", "-m", "--setnchn", str(nchan), outname]
        if tsub is not None:
            cmd += ["--settsub", "%f" % tsub]
        else:
            cmd += ["--setnsub", "%d" % nsub]

        if nbin is not None:
            cmd += ["--setnbin", "%d" % nbin]

        # Scrunch the heck out of it
        utils.execute(cmd)

    def _add_arguments(self, parser):
        """Add any arguments to subparser that are required 
            by the manipulator.
 
            Inputs:
                parser: An argparse subparser for which arguments should
                    be added in-place.
            
            Outputs:
                None
        """
        parser.add_argument("--nsub", type=int, dest='nsub',
                            default=1,
                            help="Number of sub-ints to scrunch to. "
                                 "(Default: 1).")
        parser.add_argument("--tsub", type=float, dest='tsub',
                            default=None,
                            help="Number of seconds to include in each "
                                 "sub-int. This will override --nsub. "
                                 "(Default: 1 sub-int per obs).")
        parser.add_argument("--nchan", type=int, dest='nchan',
                            default=1,
                            help="Number of sub-bands to scrunch to. "
                                 "(Default: 1).")
        parser.add_argument("--nbin", type=int, dest='nbin',
                            default=argparse.SUPPRESS,
                            help="Number of bins to scrunch to. "
                                 "(Default: don't bin scrunch).")

Manipulator = PamitManipulator

