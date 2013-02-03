import tempfile
import os
import shutil

import utils
import base

class CompositePlotDiagnostic(base.PlotDiagnostic):
    name = 'Composite'

    def _compute(self):
        utils.print_info("Creating composite summary plot for %s" % self.fn, 3)
        handle, tmpfn = tempfile.mkstemp(suffix=".png")
        os.close(handle)
        params = utils.prep_file(self.fn)
        utils.execute("psrplot -O -j 'D' -c 'above:c=,x:range=0:2' %s -D %s/PNG " \
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
                            (self.fn, tmpfn, os.path.split(self.fn)[-1], \
                                params['telescop'], params['rcvr'], \
                                params['backend'], params['length'], params['bw']))
        tmpdir = os.path.split(tmpfn)[0]
        archivefn = os.path.split(self.fn)[-1]
        pngfn = os.path.join(tmpdir, archivefn+".composite.png")
        shutil.move(tmpfn, pngfn) 
        return pngfn


Diagnostic = CompositePlotDiagnostic
