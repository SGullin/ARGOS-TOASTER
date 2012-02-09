#!/usr/bin/env python
import glob

import config
import epta_pipeline_utils as epu

def get_header_params(fn):
    """Get a small set of header params that we might change.
        Returns a dictionary.

        Inputs:
            fn: The name of the file to get params for.

        Output:
            params: A dictionary. The keys are values requested from 'psredit'
                the values are the values reported by 'psredit'.
    """
    params_to_get = ['rcvr:name', 'be:name', 'site']
    cmd = "psredit -q -Q -c '%s' %s" % (','.join(params_to_get), fn)
    outstr, errstr = epu.execute(cmd)

    params = {}
    for key, val in zip(params_to_get, outstr.split()):
        params[key] = val
    return params


def set_header_param(fn, param, val):
    """Set a header paramter using 'psredit'.
        
        Inputs:
            fn: The name of the file to set param for.
            param: The 'psredit'-compatible parameter name.
            val: The value to set the parameter as.

        Outputs:
            None
    """
    cmd = "psredit -m -c '%s=%s' %s" % (param, val, fn)
    epu.execute(cmd)


def convert_file_to_psrfits(fn):
    """Convert the given file to PSRFITS format using 'psrconv'.
        This change is done in place.

        Input:
            fn: The name of the file to convert.

        Outputs:
            None
    """
    cmd = "psrconv -m -o PSRFITS %s" % fn
    epu.execute(cmd)


def main():
    # Collect input files
    infiles = set(args.infiles)
    for glob_expr in args.glob_exprs:
        infiles.update(glob.glob(glob_expr))
    infiles = list(infiles)

    for fn in sorted(infiles):
        params = get_header_params(fn)
        print "%s:" % fn
        made_changes = False
      
        # Convert archive to PSRFITS format
        if args.convert:
            print "    Convert to PSRFITS"
            if not args.dry_run:
                convert_file_to_psrfits(fn)
            made_changes = True

        # Correct receiver
        if (args.receiver is not None) and \
                (args.force or (params['rcvr:name'] == args.old_receiver)):
            print "    rcvr:name -- %s -> %s" % (params['rcvr:name'], args.receiver)
            if not args.dry_run:
                set_header_param(fn, 'rcvr:name', args.receiver)
            made_changes = True
        # Correct backend
        if (args.backend is not None) and \
                (args.force or (params['be:name'] == args.old_backend)):
            print "    be:name -- %s -> %s" % (params['be:name'], args.backend)
            if not args.dry_run:
                set_header_param(fn, 'be:name', args.backend)
            made_changes = True

        # Print a msg if no changes
        if not made_changes:
            print "    No changes to be made"
        elif args.dry_run:
            print "    No changes were made (dry run)"
            

if __name__ == '__main__':
    parser = epu.DefaultArguments()
    parser.add_argument("infiles", nargs='*', action='store', \
                        help="Files with headers to correct.")
    parser.add_argument("--convert", action='store_true', \
                        dest='convert', default=False, \
                        help="First convert file to PSRFITS format " \
                             "using 'psrconv'. NOTE: Conversion may be " \
                             "neccessary. (Default: Don't convert format.)")
    parser.add_argument("-r", "--receiver", action='store', \
                        dest='receiver', default=None, type=str, \
                        help="Corrected receiver name. " \
                             "'rcvr:name' will be set to this.")
    parser.add_argument("--old-receiver", action='store', \
                        dest='old_receiver', default='unknown', type=str, \
                        help="Only change the receiver name if it is " \
                             "currently set to this. (Default: 'unknown')")
    parser.add_argument("-b", "--backend", action='store', \
                        dest='backend', default=None, type=str, \
                        help="Corrected backend name. " \
                             "'be:name' will be set to this.")
    parser.add_argument("--old-backend", action='store', \
                        dest='old_backend', default='unknown', type=str, \
                        help="Only change the backend name if it is " \
                             "currently set to this. (Default: 'unknown')")
    parser.add_argument("-n", "--dry-run", action="store_true", \
                        dest='dry_run', default=False, \
                        help="Perform a dry run. Only print what updates " \
                             "would take place. (Default: make updates.)")
    parser.add_argument("-f", "--force", action="store_true", \
                        dest='force', default=False, \
                        help="Forcefully update parameters. (Default: " \
                             "only update a parameter if its value is 'unknown'.)")
    parser.add_argument("-g", "--glob-files", action="append", \
                        dest='glob_exprs', default=[], \
                        help="Glob expression identifying files with " \
                             "headers to correct. Be sure to correctly " \
                             "quote the expression.")
    args = parser.parse_args()
    main()

