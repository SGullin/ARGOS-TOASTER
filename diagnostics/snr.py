import utils
import base

class SNRDiagnostic(base.FloatDiagnostic):
    name = "SNR"
    description = "Signal-to-noise ratio for the fully scrunched " \
                    "data as determined by 'psrstat'."

    def _compute(self):
        utils.print_info("Calling psrstat to get SNR for %s" % self.fn, 3)
        cmd = ["psrstat", "-Qq", "-j", "DTFp", "-c", "snr", self.fn]
        outstr, errstr = utils.execute(cmd)
        snr = float(outstr)
        return snr


Diagnostic = SNRDiagnostic
