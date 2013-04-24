import tempfile
import os
import shutil

import utils
import errors
import base

class FreqVsPhasePlotDiagnostic(base.PlotDiagnostic):
    name = 'Freq vs. Phase'

    def _compute(self):
        utils.print_info("Creating freq vs. phase plot for %s" % self.fn, 3)
        params = utils.prep_file(self.fn)
        if not (params['nchan'] > 1):
            raise errors.DiagnosticNotApplicable("Archive (%s) only has " \
                        "a single channel. Freq vs. phase diagnostic " \
                        "doesn't apply to this data file." % self.fn)
    
        handle, tmpfn = tempfile.mkstemp(suffix=".png")
        os.close(handle)
        utils.execute("psrplot -p freq -j DTp -c 'above:c=%s' " \
                                    "-D %s/PNG %s" % \
                        (os.path.split(self.fn)[-1], tmpfn, self.fn))
        tmpdir = os.path.split(tmpfn)[0]
        pngfn = os.path.join(tmpdir, self.fn+".freq.png")
        shutil.move(tmpfn, pngfn) 
        return pngfn


Diagnostic = FreqVsPhasePlotDiagnostic

