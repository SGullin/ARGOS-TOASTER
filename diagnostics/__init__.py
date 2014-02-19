import os.path

from toaster import config
from toaster import errors
from toaster.diagnostics import base
from toaster.utils import datafile
from toaster.utils import notify


registered_diagnostics = ['composite',
                          'snr',
                          'time_vs_phase',
                          'freq_vs_phase',
                          'profile',
                          'stokes',
                          'masked_percentage',
                          ]


def get_diagnostic_class(diagnostic_name):
    """Given a diagnostic name return the corresponding diagnostic
        class.

        Inputs:
            diagnostic_name: A diagnostic name.
            
        Outputs:
            diagcls: The diagnostic class.
    """
    if diagnostic_name not in registered_diagnostics:
        raise errors.UnrecognizedValueError("The diagnostic, '%s', "
                                            "is not a registered. "
                                            "The following are registered: "
                                            "'%s'" %
                                            (diagnostic_name,
                                             "', '".join(registered_diagnostics)))
    mod = __import__(diagnostic_name, globals())
    diagcls = mod.Diagnostic
    return diagcls


def get_custom_float_diagnostic(fn, diagnostic_name, diagnostic_value):
    """Create and return a custom float diagnostic object.
    
        Inputs:
            fn: The name of the file being diagnosed.
            diagnostic_name: A diagnostic name.
            diagnostic_value: The diagnostic's value.

        Outputs:
            diagnostic: The computed diagnostic object.
    """
    return CustomFloatDiagnostic(fn, diagnostic_name, diagnostic_value)


def get_custom_diagnostic_plot(fn, diagnostic_name, diagnostic_plotfn):
    """Create and return a custom diagnostic plot object.
    
        Inputs:
            fn: The name of the file being diagnosed.
            diagnostic_name: A diagnostic name.
            diagnostic_plotfn: The diagnostic plot's filename/path.

        Outputs:
            diagnostic: The computed diagnostic object.
    """
    return CustomPlotDiagnostic(fn, diagnostic_name, diagnostic_plotfn)


class CustomFloatDiagnostic(base.FloatDiagnostic):
    """A FloatDiagnostic object for custom (one-time use) diagnostics.
    """
    def __init__(self, fn, name, value):
        self.name = name
        self.value = value
        super(CustomFloatDiagnostic, self).__init__(fn)

    def _compute(self):
        return self.value


class CustomPlotDiagnostic(base.PlotDiagnostic):
    """A PlotDiagnostic object for custom (one-time use) diagnostics.
    """
    def __init__(self, fn, name, plotfn):
        self.name = name
        if not os.path.isfile(plotfn):
            raise errors.FileError("The diagnostic plot provided (%s) "
                                   "doesn't exist!" % plotfn)
        self.plotfn = plotfn
        super(CustomPlotDiagnostic, self).__init__(fn)

    def _compute(self):
        return self.plotfn