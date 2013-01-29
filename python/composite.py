import utils
import shutil
import tempfile
import sys
import os

def make_composite_summary_plot(arfn, preproc='D', outpsfn=None):
    utils.print_info("Creating composite summary plot for %s" % arfn, 1)
    if outpsfn is None:
        outpsfn = "%s.ps" % arfn
    utils.print_info("Output plot name: %s" % outpsfn, 2)
    handle, tmpfn = tempfile.mkstemp(suffix=".ps")
    os.close(handle)
    params = utils.prep_file(arfn)
    utils.execute("psrplot -O -j '%s' -c 'above:c=,x:range=0:2' %s -D %s/CPS " \
                    "-p flux -c ':0:x:view=0.575:0.95," \
                                   "y:view=0.7:0.9," \
                                   "subint=I," \
                                   "chan=I," \
                                   "pol=I," \
                                   "x:opt=BCTS," \
                                   "x:lab=," \
                                   "below:l=' " \
                    "-p freq -c ':1:x:view=0.075:0.45," \
                                   "y:view=0.15:0.7," \
                                   "subint=I," \
                                   "pol=I," \
                                   "above:l=%s\n" \
                                           "%s - %s (%s)\n" \
                                           "Length=%.1f s - BW=%.1f MHz\n" \
                                           "$nbin bins - $nchan chans - $nsubint subints," \
                                   "above:off=3.5," \
                                   "cmap:map=plasma' " \
                    "-p time -c ':2:x:view=0.575:0.95," \
                                   "y:view=0.15:0.7," \
                                   "chan=I," \
                                   "pol=I," \
                                   "cmap:map=plasma'" % \
                        (preproc, arfn, tmpfn, os.path.split(arfn)[-1], \
                            params['telescop'], params['rcvr'], params['backend'], \
                            params['length'], params['bw']))
    # Rename tmpfn to requested output filename
    shutil.move(tmpfn, outpsfn)


if __name__ == '__main__':
    make_composite_summary_plot(sys.argv[1])
