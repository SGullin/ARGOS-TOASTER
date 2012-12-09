"""
This file contains custom errors for TOASTER.

Patrick Lazarus, Feb. 9, 2012
"""

import colour

# Fatal class of TOASTER errors. These should not be caught.
class FatalToasterError(Exception):
    def __str__(self):
       return colour.cstring(super(FatalToasterError, self).__str__(), 'error')


class NoConfigError(FatalToasterError):
    pass


class BadColumnNameError(FatalToasterError):
    pass


# Regular TOASTER errors. These might be caught and dealt with.
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


class RawfileSuperseded(ToasterError):
    pass


# Custom Warnings
class ToasterWarning(Warning):
    def __str__(self):
        return colour.cstring(super(ToasterWarning, self).__str__(), 'warning')
