import tempfile
import os
import shutil

import utils
import base

class StokesPlotDiagnostic(base.PlotDiagnostic):
    name = 'Profile (w/ pol)'

    def _compute(self):
        utils.print_info("Creating profile plot (w/ polarization) for %s" % self.fn, 3)
        params = utils.prep_file(self.fn)
        handle, tmpfn = tempfile.mkstemp(suffix=".png")
        os.close(handle)
        utils.execute("psrplot -p stokes -j TDF -c 'above:c=%s' " \
                                    "-D %s/PNG %s" % \
                        (os.path.split(archivefn)[-1], tmpfn, archivefn))
        tmpdir = os.path.split(tmpfn)[0]
        pngfn = os.path.join(tmpdir, self.fn+".stokes.png")
        shutil.move(tmpfn, pngfn) 
        return pngfn


Diagnostic = StokesPlotDiagnostic

