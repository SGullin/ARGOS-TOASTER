"""
This file contains custom errors for the EPTA timing pipeline.

Patrick Lazarus, Feb. 9, 2012
"""

import colour

class EptaPipelineError(Exception):
    def __str__(self):
       return colour.cstring(super(EptaPipelineError, self).__str__(), 'error')


class SystemCallError(EptaPipelineError):
    pass


class FileError(EptaPipelineError):
    pass


class UnrecognizedValueError(EptaPipelineError):
    pass


class DatabaseError(EptaPipelineError):
    pass


# Custom Warnings
class EptaPipelineWarning(Warning):
    def __str__(self):
        return colour.cstring(super(EptaPipelineWarning, self).__str__(), 'warning')
