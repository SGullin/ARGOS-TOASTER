import numpy as np

from toaster import utils
from toaster.utils import notify
from toaster.diagnostics import base


class MaskedPercentageDiagnostic(base.FloatDiagnostic):
    name = "Masked Percentage"
    description = "Percentage of profiles that are fully masked."

    def _compute(self):
        notify.print_info("Calling psrstat to get weights for %s" % self.fn, 3)
        cmd = ["psrstat", self.fn, "-c", "int:wt", "-Qq"]
        outstr, errstr = utils.execute(cmd)
        wtstrs = outstr.strip().split(',')
        weights = np.array([float(wt) for wt in wtstrs])
        maskpcnt = 100.0*np.sum(weights > 0)/weights.size
        return maskpcnt


Diagnostic = MaskedPercentageDiagnostic
