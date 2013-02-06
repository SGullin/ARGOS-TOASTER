import os.path

import database
import utils
import errors
import base

registered_diagnostics = ['composite', \
                          'snr', \
                          'time_vs_phase', \
                          ]


def get_diagnostic(diagnostic_name, *args, **kwargs):
    """Given a diagnostic name return a computed diagnostic object.

        Inputs:
            diagnostic_name: A diagnostic name.
            
            ** Additional arguments are passed onto the diagnostic constructor.

        Outputs:
            diagnostic: The computed diagnostic object.
    """
    if diagnostic_name not in registered_diagnostics:
        raise errors.UnrecognizedValueError("The diagnostic, '%s', " \
                    "is not a registered. The following " \
                    "are registered: '%s'" % \
                    (diagnostic_name, "', '".join(registered_diagnostics)))
    mod = __import__(diagnostic_name, globals())
    diag = mod.Diagnostic(*args, **kwargs)
    return diag


def get_custom_float_diagnostic(diagnostic_name, diagnostic_value, rawfile_id):
    """Create and return a custom float diagnostic object.
    
        Inputs:
            diagnostic_name: A diagnostic name.
            diagnostic_value: The diagnostic's value.
            rawfile_id: The ID of the rawfile to compute diagnostics for.

        Outputs:
            diagnostic: The computed diagnostic object.
    """
    return CustomFloatDiagnostic(rawfile_id, diagnostic_name, diagnostic_value)


def get_custom_diagnostic_plot(diagnostic_name, diagnostic_plotfn, rawfile_id):
    """Create and return a custom diagnostic plot object.
    
        Inputs:
            diagnostic_name: A diagnostic name.
            diagnostic_plotfn: The diagnostic plot's filename/path.
            rawfile_id: The ID of the rawfile to compute diagnostics for.

        Outputs:
            diagnostic: The computed diagnostic object.
    """
    return CustomPlotDiagnostic(rawfile_id, diagnostic_name, diagnostic_plotfn)


class CustomFloatDiagnostic(base.FloatDiagnostic):
    """A FloatDiagnostic object for custom (one-time use) diagnostics.
    """
    def __init__(self, rawfile_id, name, value):
        self.name = name
        self.value = value
        super(CustomFloatDiagnostic, self).__init__(rawfile_id)

    def _compute(self):
        return self.value


class CustomPlotDiagnostic(base.PlotDiagnostic):
    """A PlotDiagnostic object for custom (one-time use) diagnostics.
    """
    def __init__(self, rawfile_id, name, plotfn):
        self.name = name
        if not os.path.isfile(plotfn):
            raise errors.FileError("The diagnostic plot provided (%s) " \
                            "doesn't exist!" % plotfn)
        self.plotfn = plotfn
        super(CustomPlotDiagnostic, self).__init__(rawfile_id)

    def _compute(self):
        return self.plotfn



