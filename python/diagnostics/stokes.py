import tempfile
import os
import shutil

import utils
import base

class StokesPlotDiagnostic(base.PlotDiagnostic):
    name = 'Profile (w/ pol)'
    description = "Fully scrunched profile with polarisation information " \
                    "shown."

    def _compute(self):
        utils.print_info("Creating profile plot (w/ polarization) for %s" % self.fn, 3)
        params = utils.prep_file(self.fn)
        handle, tmpfn = tempfile.mkstemp(suffix=".png")
        os.close(handle)
        cmd = ["psrplot", "-p", "stokes", "-j", "TDF", "-c", \
                "above:c=%s" % os.path.split(archivefn)[-1], \
                "-D", "%s/PNG" % tmpfn, archivefn]
        utils.execute(cmd)
        tmpdir = os.path.split(tmpfn)[0]
        pngfn = os.path.join(tmpdir, self.fn+".stokes.png")
        shutil.move(tmpfn, pngfn) 
        return pngfn


Diagnostic = StokesPlotDiagnostic

