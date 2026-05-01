"""
InLaw - A lightweight wrapper around Great Expectations (GX)

Provides simple database validation testing with minimal boilerplate.
"""

from src.inlaw import InLaw, InlawError
from src.dbtable import DBTable, DBTableError, DBTableValidationError, DBTableHierarchyError

__version__ = "0.1.0"

__all__ = [
    "InLaw",
    "InlawError",
    "DBTable",
    "DBTableError",
    "DBTableValidationError",
    "DBTableHierarchyError",
]
