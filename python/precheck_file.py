#!/usr/bin/env python
import argparse
import sys
import glob

import epta_pipeline_utils as epu
import errors
import config
import load_rawfile

def main():
    # Collect input files
    infiles = set(args.infiles)
    for glob_expr in args.glob_exprs:
        infiles.update(glob.glob(glob_expr))
    infiles = list(infiles)

    if not infiles:
        sys.stderr.write("You didn't provide any files to load. " \
                         "You should consider including some next time...\n")
        sys.exit(1)
    
    # Enter information in rawfiles table
    # create diagnostic plots and metrics.
    # Also fill-in raw_diagnostics and raw_diagnostic_plots tables
    for fn in infiles:
        try:
            if config.verbosity:
                print "Checking %s (%s)" % (fn, epu.Give_UTC_now())
            # Check the file and parse the header
            params = load_rawfile.prep_file(fn)
            
            # Move the File
            destdir = epu.get_archive_dir(fn, site=params['site'], \
                        backend=params['be:name'], psrname=params['name'])
            
            print_info("%s will get archived to %s (%s)" % \
                        (fn, destdir, epu.Give_UTC_now()), 1)

            print_info("Finished with %s - pre-check successful (%s)" % \
                        (fn, epu.Give_UTC_now()), 1)

        except errors.EptaPipelineError, msg:
            sys.stderr.write("Pre-check of %s failed!\n%s.\nSkipping...\n" % \
                                (fn, msg))
    

if __name__=='__main__':
    parser = epu.DefaultArguments(description="Pre-check raw files.")
    parser.add_argument("infiles", nargs='*', action='store', \
                        help="Files with headers to check.")
    parser.add_argument("-g", "--glob-files", action="append", \
                        dest='glob_exprs', default=[], \
                        help="Glob expression identifying files with " \
                             "headers to check. Be sure to correctly " \
                             "quote the expression. Multiple -g/--glob-files " \
                             "options can be provided.")
    args = parser.parse_args()
    main()
