#!/usr/bin/env python
import os.path

import utils
import errors

from toolkit.timfiles import readers

SHORTNAME = "load"
DESCRIPTION = "Load a TOA created outside of TOASTER."

# Reader function
READERS = {'tempo2': readers.tempo2_reader, \
            }

def add_arguments(parser):
    parser.add_argument('--timfile', dest='timfile', \
                        type=str, default=None, \
                        help="A list of TOAs (one per line) to load.")
    parser.add_argument('-f', '--format', dest='format', \
                        default='tempo2', type=str, \
                        help="Input format for the timfile. " \
                            "Available formats: '%s'. (Default: " \
                            "tempo2)" % "', '".join(sorted(READERS)))


def parse_timfile(timfn, reader=readers.tempo2_reader):
    """Read the input timfile and parse the TOAs contained.

        Inputs:
            timfn: The timfile to parse.
            reader: The reader function. Reader functions take
                a single line as input and return a dictionary
                of TOA info. (Default: a Tempo2 TOA format reader)

        Output:
            toas: A list of TOA info dictionaries.
    """
    if not os.path.exists(timfn):
        raise errors.FileError("The input timfile (%s) does not " \
                        "appear to exist." % timfn)
    utils.print_info("Starting to parse timfile (%s)" % timfn, 2)
    toas = []
    timfile = open(timfn, 'r')
    for ii, line in enumerate(timfile):
        line = line.strip()
        if line.startswith("INCLUDE"):
            toas.extend(parse_timfile(line.split()[1], reader=reader))
        try:
            toainfo = reader(line)
        except Exception, e:
            raise errors.BadTOAFormat("Error occurred while parsing " \
                                    "TOA line (%s:%d):\n    %s\n\n" \
                                    "Original exception message:\n    %s" % \
                                    (timfn, ii, line, str(e)))
        if toainfo is not None:
            toas.append(toainfo)
    utils.print_info("Finished parsing timfile (%s). Read %d TOAs." % \
                        (timfn, len(toas)), 2)
    return toas


def main(args):
    if args.timfile is None:
        raise errors.BadInputError("An input timfile is required.")
    if args.format not in READERS:
        raise errors.UnrecognizedValueError("The requested timfile format " \
                        "'%s' is not recognized. Available formats: '%s'." % \
                        (args.format, "', '".join(sorted(READERS.keys()))))
    reader = READERS[args.format]
    # Parse input file
    toas = parse_timfile(args.timfile, reader=reader)



if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
