#!/usr/bin/env python
import os.path
import types

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
    parser.add_argument('-n', '--dry-run', dest='dry_run', \
                        action='store_true', default=False, \
                        help="Print information about the TOAs, but " \
                            "don't actually load them.")
    parser.add_argument('-p', '--psr', dest='pulsar_name', \
                        type=str, default=None, \
                        help='Pulsar name, or alias. NOTE: This option ' \
                            'must be provided.')
    parser.add_argument('--backend-flag', dest='backend_flags', \
                        type=str, default=[], action='append', \
                        help='The TOA flag containing the backend name. ' \
                            'NOTE: multiple possible backend flags may ' \
                            'be specified by giving multiple instances ' \
                            'of the --backend-flag option. However, ' \
                            'only one flag may apply the TOA.')
    parser.add_argument('--backend', dest='backend', \
                        type=str, default=None, \
                        help='The name of the backend. NOTE: This value ' \
                            'will be overrided by any TOA flags that specify ' \
                            'the backend name (see --backend-flag)')
    parser.add_argument('--frontend-flag', dest='frontend_flags', \
                        type=str, default=[], action='append', \
                        help='The TOA flag containing the frontend name. ' \
                            'NOTE: multiple possible frontend flags may ' \
                            'be specified by giving multiple instances ' \
                            'of the --frontend-flag option. However, ' \
                            'only one flag may apply the TOA.')
    parser.add_argument('--frontend', dest='frontend', \
                        type=str, default=None, \
                        help='The name of the frontend. NOTE: This value ' \
                            'will be overrided by any TOA flags that specify ' \
                            'the frontend name (see --frontend-flag)')
    parser.add_argument('--obssystem-flag', dest='obssystem_flags', \
                        type=str, default=[], action='append', \
                        help='The TOA flag containing the obssystem name. ' \
                            'NOTE: multiple possible obssystem flags may ' \
                            'be specified by giving multiple instances ' \
                            'of the --obssystem-flag option. However, ' \
                            'only one flag may apply the TOA.')
    parser.add_argument('--obssystem', dest='obssystem', \
                        type=str, default=None, \
                        help='The name of the obssystem. NOTE: This value ' \
                            'will be overrided by any TOA flags that specify ' \
                            'the obssystem name (see --obssystem-flag)')


def __parse_timfile(timfn, reader=readers.tempo2_reader, \
                    **obssys_discovery_kwargs):
    """Read the input timfile and parse the TOAs contained.

        Inputs:
            timfn: The timfile to parse.
            reader: The reader function. Reader functions take
                a single line as input and return a dictionary
                of TOA info. (Default: a Tempo2 TOA format reader)
            
            ** Additional keyword arguments are directly passed on 
                to __determine_obssystem(...) for observing system 
                discovery

        Output:
            toas: A list of TOA info dictionaries.
    """
    if not os.path.exists(timfn):
        raise errors.FileError("The input timfile (%s) does not " \
                        "appear to exist." % timfn)
    if type(reader) is types.StringType:
        # Assume reader is actually the name of the reader.
        if reader not in READERS:
            raise errors.UnrecognizedValueError("Requested reader (%s) is " \
                        "not recognized!" % reader)
        else:
            reader = READERS[reader]
    utils.print_info("Starting to parse timfile (%s)" % timfn, 2)
    toas = []
    timfile = open(timfn, 'r')
    for ii, line in enumerate(timfile):
        line = line.strip()
        if line.startswith("INCLUDE"):
            # Recursively parse included files
            toas.extend(__parse_timfile(line.split()[1], reader=reader, \
                            **obssys_discovery_kwargs))
        try:
            toainfo = reader(line)
            if toainfo is not None:
                toainfo['obssystem_id'] = __determine_obssystem(toainfo, \
                                                **obssys_discovery_kwargs)
                toas.append(toainfo)
        except Exception, e:
            raise errors.BadTOAFormat("Error occurred while parsing " \
                                    "TOA line (%s:%d):\n    %s\n\n" \
                                    "Original exception message:\n    %s" % \
                                    (timfn, ii+1, line, str(e)))
    utils.print_info("Finished parsing timfile (%s). Read %d TOAs." % \
                        (timfn, len(toas)), 2)
    return toas


def __determine_obssystem(toainfo, obssystem_name=None, obssystem_flags=[], \
                                 frontend_name=None, frontend_flags=[], \
                                 backend_name=None, backend_flags=[]):
    """Given a TOA determine its observing system, either
        from default values, or by considering TOA flags.
    
        Inputs:
            toainfo: A TOA info dictionary.
            obssystem_name: The default observing system name. 
                (Default: None)
            obssystem_flags: A list of TOA flag names that will
                have the observing system's name as a value.
                (Default: No flags)
            frontend_name: The default observing system name. 
                (Default: None)
            frontend_flags: A list of TOA flag names that will
                have the observing system's name as a value.
                (Default: No flags)
            backend_name: The default observing system name. 
                (Default: None)
            backend_flags: A list of TOA flag names that will
                have the observing system's name as a value.
                (Default: No flags)
            
        Output:
            obssysid: The observing system's ID.

        NOTE: If the observing system cannot be determined, or
            if flags contradict each other, an exception is 
            raised.

    """
    # Check that enough command line arguments are given to discover
    # The observing system
    if (obssystem_name or obssystem_flags) or \
            ((backend_name or backend_flags) and \
             (frontend_name or frontend_flags)):
        # Enough information has be provided to discover obssystems
        pass
    else:
        raise errors.BadInputError("Not enough information has been " \
                        "provided to determine the observing system " \
                        "of TOAs.")

    # Determine observing system
    # First try to determine obssystem directly
    matching_keys = [key for key in toainfo['flags'] if key in obssystem_flags]
    if len(matching_keys) == 1:
        obssysname = toainfo['flags'][matching_keys[0]]
    elif len(matching_keys) == 0:
        obssysname = obssystem_name
    else:
        raise errors.BadTOAFormat("Too many obssystem flags match " \
                            "TOA line (%s:%d):\n    %s" % \
                            (timfn, ii+1, line))
    # Check consistency with telescope code
    if obssysname is not None:
        obssysid = utils.get_obssysid(obssysname)
        obssysinfo = utils.get_obssysinfo(obssysid)
        if toainfo['telescope_id'] != obssysinfo['telescope_id']:
            raise errors.BadTOAFormat("Telescope from obs code doesn't "
                                    "match observing system! TOA line " \
                                    "(%s:%d):\n    %s" % (timfn, ii+1, line))

    # Now use frontend/backend/telescope
    # Get frontend
    matching_keys = [key for key in toainfo['flags'] if key in frontend_flags]
    if len(matching_keys) == 1:
        fename = toainfo['flags'][matching_keys[0]]
    elif len(matching_keys) == 0:
        fename = frontend_name
    else:
        raise errors.BadTOAFormat("Too many frontend flags match " \
                            "TOA line (%s:%d):\n    %s" % \
                            (timfn, ii+1, line))
    if (fename is not None) and (obssysname is not None):
        if fename != obssysinfo['frontend']:
            raise errors.BadTOAFormat("Frontend from flag doesn't match " \
                                    "observing system! TOA line " \
                                    "(%s:%d):\n    %d" % (timfn, ii+1, line))
    # Get backend
    matching_keys = [key for key in toainfo['flags'] if key in backend_flags]
    if len(matching_keys) == 1:
        bename = toainfo['flags'][matching_keys[0]]
    elif len(matching_keys) == 0:
        bename = backend_name
    else:
        raise errors.BadTOAFormat("Too many backend flags match " \
                            "TOA line (%s:%d):\n    %s" % \
                            (timfn, ii+1, line))
    if (bename is not None) and (obssysname is not None):
        if bename != obssysinfo['backend']:
            raise errors.BadTOAFormat("Backend from flag doesn't match " \
                                    "observing system! TOA line " \
                                    "(%s:%d):\n    %d" % (timfn, ii+1, line))

    if (bename is not None) and (fename is not None):
        obssysid = utils.get_obssysid((toainfo['telescope'], fename, bename))
    return obssysid


def load_from_timfile(timfile, pulsar_id, reader='tempo2', \
                        **obssystem_discovery_args):
    """Load TOAs from a timfile.

        Inputs:
            timfile: The timfile to parse TOAs out of.
            pulsar_id: The ID number of the pulsar the
                TOAs belong to.
            reader: The reader function to use, or the
                key from the READERS dictionary corresponding
                to the function to use.
            
            ** Additional keyword arguments are directly passed on 
                to __determine_obssystem(...) for observing system 
                discovery

        Outputs:
            toas: The TOAs that were loaded into the DB.
    """
    # Parse input file
    toas = __parse_timfile(timfile, reader=reader, \
                            **obssystem_discovery_args)
    for ti in toas:
        ti['pulsar_id'] = pulsar_id
    utils.load_toas(toas)
    return toas


def main(args):
    if args.timfile is None:
        raise errors.BadInputError("An input timfile is required.")
    if args.format not in READERS:
        raise errors.UnrecognizedValueError("The requested timfile format " \
                        "'%s' is not recognized. Available formats: '%s'." % \
                        (args.format, "', '".join(sorted(READERS.keys()))))
    pulsar_id = utils.get_pulsarid(args.pulsar_name)
   
    obssystem_discovery_args = {'obssystem_name':args.obssystem, \
                                'obssystem_flags':args.obssystem_flags, \
                                'backend_name':args.backend, \
                                'backend_flags':args.backend_flags, \
                                'frontend_name':args.frontend, \
                                'frontend_flags':args.frontend_flags}

    if args.dry_run:
        # Parse input file
        toas = __parse_timfile(args.timfile, reader=args.format, \
                                **obssystem_discovery_args)
        print "%d TOAs parsed" % len(toas)
    else:
        load_from_timfile(args.timfile, pulsar_id=pulsar_id, \
                                reader=args.format, \
                                **obssystem_discovery_args)
        

if __name__=='__main__':
    parser = utils.DefaultArguments(description=DESCRIPTION)
    add_arguments(parser)
    args = parser.parse_args()
    main(args)
