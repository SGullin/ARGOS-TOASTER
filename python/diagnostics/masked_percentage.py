import numpy as np

import utils
import base

class MaskedPercentageDiagnostic(base.FloatDiagnostic):
    name = "Masked Percentage"

    def _compute(self):
        utils.print_info("Calling psrstat to get weights for %s" % self.fn, 3)
        cmd = "psrstat %s -c int:wt -Qq" % self.fn
        outstr, errstr = utils.execute(cmd)
        wtstrs = outstr.strip().split(',')
        weights = np.array([float(wt) for wt in wtstrs])
        maskpcnt = 100.0*np.sum(weights>0)/weights.size
        return maskpcnt


Diagnostic = MaskedPercentageDiagnostic
