import sys
import argparse

import manipulators

def main():
    manipulators.run_manipulator(args.func, args.infiles, args, \
                                    outname=args.outfile)

if __name__=='__main__':
    parser = argparse.ArgumentParser(description="Call a manipulator plug-in's " \
                                        "manipulate method with the given " \
                                        "arguments on the given files.")
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument('infiles', metavar="INFILES", \
                                nargs='+', type=str, \
                                help="Archive files to be manipulated.")
    parent_parser.add_argument('-o', '--outfile', type=str, dest='outfile', \
                              help="Name of file to output the resulting "
                                    "manipulated archive to. (Default: " \
                                    "don't write output file to disk.)", \
                                default=None)
        
    subparsers = parser.add_subparsers(dest='manipulator', \
                                        title="Manipulators")
    for name in manipulators.registered_manipulators:
        m = manipulators.__dict__[name]
        m_parser = subparsers.add_parser(m.plugin_name, help=m.__doc__, \
                                            parents=[parent_parser])
        m.add_arguments(m_parser)
        m_parser.set_defaults(func=m.manipulate)
    args = parser.parse_args()
    # Debugging
    # print args
    main()
