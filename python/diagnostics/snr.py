import utils
import base

class SNRDiagnostic(base.FloatDiagnostic):
    name = "SNR"

    def _compute(self):
        utils.print_info("Calling psrstat to get SNR for %s" % self.fn, 3)
        cmd = "psrstat -Qq -j DTFp -c 'snr' %s" % self.fn
        outstr, errstr = utils.execute(cmd)
        snr = float(outstr)
        return snr


Diagnostic = SNRDiagnostic
