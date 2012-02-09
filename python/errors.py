"""
This file contains custom errors for the EPTA timing pipeline.

Patrick Lazarus, Feb. 9, 2012
"""

class EptaPipelineError(Exception):
    pass


class SystemCallError(EptaPipelineError):
    pass


class FileExistenceError(EptaPipelineError):
    pass
