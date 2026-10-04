"""
InLaw - A lightweight wrapper around Great Expectations (GX)

Provides simple database validation testing with minimal boilerplate.
"""

from .inlaw import GXValidatorAdapter, InLaw, InlawError
from .dbtable import DBTable, DBTableError, DBTableValidationError, DBTableHierarchyError

__version__ = "0.2.0"

__all__ = [
    "InLaw",
    "InlawError",
    "GXValidatorAdapter",
    "DBTable",
    "DBTableError",
    "DBTableValidationError",
    "DBTableHierarchyError",
]
