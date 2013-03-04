import os.path

import utils
import database
import errors


class BaseDiagnostic(object):
    """The base class for diagnostics.
    """
    name = NotImplemented
    description = None

    def __init__(self, fn):
        if not os.path.isfile(fn):
            raise errors.FileError("Input file (%s) doesn't exist!" % fn)
        self.fn = fn
        self.diagnostic = self._compute()

    def _compute(self):
        raise NotImplementedError("The compute method must be defined by " \
                                    "subclasses of BaseDiagnostic.")

    def __str__(self):
        return "%s: %s" % (self.name, self.diagnostic)


class FloatDiagnostic(BaseDiagnostic):
    """The base class for floating-point valued diagnostics.
    """

class PlotDiagnostic(BaseDiagnostic):
    """The base class for plot diagnostics.
    """
