#!/usr/bin/env python
import glob
import tempfile
import warnings

import config
import errors
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
    params_to_get = ['rcvr', 'backend', 'telescop']
    return epu.get_header_vals(fn, params_to_get)


def correct_header(fn, rcvr=None, backend=None):
    """Set the receiver header paramter using 'pam'.
        
        Inputs:
            fn: The name of the file to set param for.
            rcvr: The name of the receiver.
                (Default: Do not update the receiver name.)
            backend: The name of the backend.
                (Default: Do not update the backend name.)

        Outputs:
            None
    """
    cmd = "pam -m %s" % fn
    if rcvr:
        rcvr_file = tempfile.NamedTemporaryFile(suffix='.rcvr')
        rcvr_file.write("%s\n" % rcvr)
        rcvr_file.flush()
        cmd += " --receiver %s" % (rcvr_file.name)
    if backend:
        cmd += " --inst %s" % (backend)
    if not rcvr and not backend:
        warnings.warn("Trying to correct file %s, but didn't provide " \
                        "any new header values." % fn, \
                        errors.EptaPipelineWarning)
    else:
        stdout, stderr = epu.execute(cmd)


def main():
    # Collect input files
    infiles = set(args.infiles)
    for glob_expr in args.glob_exprs:
        infiles.update(glob.glob(glob_expr))
    infiles = list(infiles)

    for fn in sorted(infiles):
        print "%s:" % fn
        made_changes = False
        
        # Get header parameters we may want to change
        params = get_header_params(fn)
      
        # Correct receiver
        if (args.receiver is not None) and \
                (args.force or (params['rcvr'] == args.old_receiver)):
            new_receiver = args.receiver
            print "    rcvr -- %s -> %s" % (params['rcvr'], args.receiver)
        else:
            new_receiver = None
        
        # Correct backend
        if (args.backend is not None) and \
                (args.force or (params['backend'] == args.old_backend)):
            new_backend = args.backend
            print "    backend -- %s -> %s" % (params['backend'], args.backend)
        else:
            new_backend = None
        
        if not args.dry_run:
            correct_header(fn, rcvr=new_receiver, backend=new_backend)
            made_changes = True

        # Print a msg if no changes
        if not made_changes:
            if args.dry_run:
                print "    No changes were made (dry run)"
            else:
                print "    No changes to be made"
            

if __name__ == '__main__':
    parser = epu.DefaultArguments(description="Change archive header " \
                        "values *IN PLACE*. The current receiver and/or " \
                        "backend values must be specified with " \
                        "--old-receiver and/or --old-backend (unless -f/" \
                        "--force is provided. NOTE: Correcting file " \
                        "headers is an irreversible process!")
    parser.add_argument("infiles", nargs='*', action='store', \
                        help="Files with headers to correct.")
    parser.add_argument("-r", "--receiver", action='store', \
                        dest='receiver', default=None, type=str, \
                        help="Corrected receiver name. " \
                             "'rcvr' will be set to this.")
    parser.add_argument("--old-receiver", action='store', \
                        dest='old_receiver', default='unknown', type=str, \
                        help="Only change the receiver name if it is " \
                             "currently set to this. (Default: 'unknown')")
    parser.add_argument("-b", "--backend", action='store', \
                        dest='backend', default=None, type=str, \
                        help="Corrected backend name. " \
                             "'backend' will be set to this.")
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

