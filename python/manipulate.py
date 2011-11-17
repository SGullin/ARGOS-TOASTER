import sys
import argparse

import manipulators

def main():
    manipulators.run_manipulator(args.func, args.infiles, **vars(args))

if __name__=='__main__':
    parser = argparse.ArgumentParser(description="Call a manipulator plug-in's " \
                                        "manipulate method with the given " \
                                        "arguments on the given files.")
    subparsers = parser.add_subparsers(dest='manipulator', \
                                        title="Manipulators")
    for name in manipulators.registered_manipulators:
        m = manipulators.__dict__[name]
        m_parser = subparsers.add_parser(m.plugin_name, \
                                        help=m.__doc__)
        m.add_arguments(m_parser)
        m_parser.set_defaults(func=m.manipulate)
        m_parser.add_argument('infiles', metavar="INFILES", \
                                 nargs='+', type=str, \
                                 help="Archive files to be manipulated.")
    
        
    args = parser.parse_args()
    print args
    main()
