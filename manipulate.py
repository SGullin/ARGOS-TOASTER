#!/usr/bin/env python

from toaster import manipulators


def main():
    manip = manipulators.load_manipulator(args.manip_name)
    manip.parse_args(leftover_args)
    manip.run(args.infiles, outname=args.outfile)


if __name__ == '__main__':
    parser = manipulators.ManipulatorArguments(
                            description="Call a manipulator plugin's "
                                        "manipulate method with the given "
                                        "arguments on the given files.")
    parser.add_argument('infiles', metavar="INFILES",
                        nargs='+', type=str,
                        help="Archive files to be manipulated.")
    parser.add_argument('-o', '--outfile', type=str, dest='outfile',
                        help="Name of file to output the resulting "
                             "manipulated archive to. (Default: "
                             "don't write output file to disk.)",
                        default=None)
    args, leftover_args = parser.parse_known_args()
    main()
