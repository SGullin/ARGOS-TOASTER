"""
This file contains custom errors for the EPTA timing pipeline.

Patrick Lazarus, Feb. 9, 2012
"""

import colour

class ToasterError(Exception):
    def __str__(self):
       return colour.cstring(super(ToasterError, self).__str__(), 'error')


class SystemCallError(ToasterError):
    pass


class FileError(ToasterError):
    pass


class UnrecognizedValueError(ToasterError):
    pass


class DatabaseError(ToasterError):
    pass


class InconsistentDatabaseError(ToasterError):
    pass


class ArchivingError(ToasterError):
    pass


class NoMasterError(ToasterError):
    pass


class BadInputError(ToasterError):
    pass


class BadDebugMode(ToasterError):
    pass


class ConflictingToasError(ToasterError):
    pass

# Custom Warnings
class ToasterWarning(Warning):
    def __str__(self):
        return colour.cstring(super(ToasterWarning, self).__str__(), 'warning')
