import tempfile
import os
import shutil

import utils
import base

class TimeVsPhasePlotDiagnostic(base.PlotDiagnostic):
    name = 'Time vs. Phase'

    def _compute(self):
        utils.print_info("Creating time vs. phase plot for %s" % self.fn, 3)
        params = utils.prep_file(self.fn)
        if not (params['nsub'] > 1):
            raise errors.DiagnosticNotApplicable("Archive (%s) only has " \
                        "a single subint. Time vs. phase diagnostic " \
                        "doesn't apply to this data file." % self.fn)
    
        handle, tmpfn = tempfile.mkstemp(suffix=".png")
        os.close(handle)
        utils.execute("psrplot -p time -j DFp -c 'above:c=%s' " \
                                    "-D %s/PNG %s" % \
                        (os.path.split(archivefn)[-1], tmpfn, archivefn))
        tmpdir = os.path.split(tmpfn)[0]
        pngfn = os.path.join(tmpdir, self.fn+".time.png")
        shutil.move(tmpfn, pngfn) 
        return pngfn


Diagnostic = TimeVsPhasePlotDiagnostic
