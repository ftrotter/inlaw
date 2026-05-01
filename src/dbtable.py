"""
DBTable - A flexible database table identifier with SQLAlchemy ORM integration.

Provides a unified way to represent database tables across different SQL dialects
while supporting both string representation for SQL queries and SQLAlchemy ORM
reflection capabilities.
"""

import re
from typing import Optional, Dict, List, Any, Union
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import DeclarativeBase
from pprint import pprint


class DBTableError(Exception):
    """Base exception for DBTable-related errors."""
    pass


class DBTableValidationError(DBTableError):
    """Raised when DBTable parameters fail validation."""
    pass


class DBTableHierarchyError(DBTableError):
    """Raised when DBTable hierarchy requirements are not met."""
    pass


class DBTable:
    """
    A flexible database table identifier that supports multiple SQL database hierarchies.
    
    Supports the hierarchy: Server/Project → Catalog → Database → Schema → Table/View
    
    Examples:
        # MySQL style (2 levels)
        table = DBTable(database='mydb', table='users')
        
        # PostgreSQL style (3 levels) 
        table = DBTable(database='mydb', schema='public', table='users')
        
        # Databricks/Spark style (3 levels)
        table = DBTable(catalog='main', database='mydb', table='users')
    """
    
    # Parameter aliases mapping - each key maps to a list of accepted parameter names
    ALIASES = {
        'catalog': ['catalog', 'catalog_name'],
        'database': ['database', 'database_name', 'db', 'db_name'],
        'schema': ['schema', 'schema_name'],
        'table': ['table', 'table_name'],
        'view': ['view', 'view_name']
    }
    
    # Hierarchy levels in order (highest to lowest)
    HIERARCHY_LEVELS = ['catalog', 'database', 'schema', 'table', 'view']
    
    # Same-level groups (cannot have multiple from same group)
    SAME_LEVEL_GROUPS = [
        {'table', 'view'}  # table and view are at the same level
    ]
    
    def __init__(self, **kwargs):
        """
        Initialize DBTable with named parameters only.
        
        Parameters must represent at least 2 different hierarchy levels.
        Names must start with a letter and contain only letters, numbers, 
        underscores, and dashes. Maximum length is 60 characters.
        
        Args:
            **kwargs: Named parameters for hierarchy levels (catalog, database, 
                     schema, table, view) and their aliases.
                     
        Raises:
            DBTableValidationError: If parameter names are invalid.
            DBTableHierarchyError: If hierarchy requirements are not met.
        """
        if not kwargs:
            raise DBTableHierarchyError("At least two hierarchy level parameters are required")
        
        # Normalize parameters using aliases
        self._normalized_params = self._normalize_parameters(kwargs)
        
        # Validate all parameter names
        for level, name in self._normalized_params.items():
            self._validate_name(name, level)
        
        # Validate hierarchy requirements
        self._validate_hierarchy()
        
        # Store the normalized parameters
        self.catalog = self._normalized_params.get('catalog')
        self.database = self._normalized_params.get('database') 
        self.schema = self._normalized_params.get('schema')
        self.table = self._normalized_params.get('table')
        self.view = self._normalized_params.get('view')
    
    def _normalize_parameters(self, kwargs: Dict[str, Any]) -> Dict[str, str]:
        """
        Normalize parameter names using the aliases mapping.
        
        Args:
            kwargs: Raw keyword arguments
            
        Returns:
            Dict mapping canonical level names to values
            
        Raises:
            DBTableValidationError: If unknown parameters are provided
        """
        normalized = {}
        used_aliases = set()
        
        for param_name, value in kwargs.items():
            # Find which canonical level this parameter belongs to
            canonical_level = None
            for level, aliases in self.ALIASES.items():
                if param_name in aliases:
                    canonical_level = level
                    break
            
            if canonical_level is None:
                raise DBTableValidationError(f"Unknown parameter: {param_name}")
            
            # Check for conflicts (multiple aliases for same level)
            if canonical_level in normalized:
                raise DBTableValidationError(
                    f"Multiple parameters provided for {canonical_level} level: "
                    f"got both {param_name} and a previous parameter"
                )
            
            normalized[canonical_level] = str(value)
            used_aliases.add(param_name)
        
        return normalized
    
    def _validate_name(self, name: str, level: str) -> None:
        """
        Validate a single name parameter.
        
        Args:
            name: The name to validate
            level: The hierarchy level (for error messages)
            
        Raises:
            DBTableValidationError: If name is invalid
        """
        if not name:
            raise DBTableValidationError(f"{level} name cannot be empty")
        
        if len(name) > 60:
            raise DBTableValidationError(
                f"{level} name '{name}' exceeds 60 character limit (got {len(name)})"
            )
        
        # Must start with a letter
        if not name[0].isalpha():
            raise DBTableValidationError(
                f"{level} name '{name}' must start with a letter"
            )
        
        # Only letters, numbers, underscores, and dashes allowed
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_-]*$', name):
            raise DBTableValidationError(
                f"{level} name '{name}' contains invalid characters. "
                "Only letters, numbers, underscores, and dashes are allowed"
            )
    
    def _validate_hierarchy(self) -> None:
        """
        Validate hierarchy requirements.
        
        Raises:
            DBTableHierarchyError: If hierarchy requirements are not met
        """
        provided_levels = set(self._normalized_params.keys())
        
        # Must have at least 2 levels
        if len(provided_levels) < 2:
            raise DBTableHierarchyError(
                f"At least 2 different hierarchy levels required, got {len(provided_levels)}: "
                f"{', '.join(provided_levels)}"
            )
        
        # Check for same-level conflicts
        for same_level_group in self.SAME_LEVEL_GROUPS:
            overlap = provided_levels.intersection(same_level_group)
            if len(overlap) > 1:
                raise DBTableHierarchyError(
                    f"Cannot specify multiple parameters from the same level: {', '.join(overlap)}"
                )
    
    def __str__(self) -> str:
        """
        Return the fully qualified table name for use in SQL queries.
        
        Returns:
            String in format like "catalog.database.schema.table"
        """
        parts = []
        
        # Build parts in hierarchy order
        for level in self.HIERARCHY_LEVELS:
            value = getattr(self, level)
            if value is not None:
                parts.append(value)
        
        return '.'.join(parts)
    
    def __repr__(self) -> str:
        """
        Return a detailed string representation for debugging.
        
        Returns:
            String showing the class and all non-None parameters
        """
        params = []
        for level in self.HIERARCHY_LEVELS:
            value = getattr(self, level)
            if value is not None:
                params.append(f"{level}='{value}'")
        
        return f"DBTable({', '.join(params)})"
    
    def make_child(self, suffix: str) -> 'DBTable':
        """
        Create a new DBTable with a suffix appended to the table/view name.
        
        Args:
            suffix: Suffix to append after an underscore
            
        Returns:
            New DBTable instance with modified table/view name
            
        Raises:
            DBTableValidationError: If no table/view is defined or suffix is invalid
        """

        suffix = suffix.lstrip('_')  # We will add the underscore back in later.

        # Validate suffix
        self._validate_name(suffix, "suffix")
        
        # Determine which name to modify (table or view)
        if self.table is not None:
            base_name = self.table
            name_type = 'table'
        elif self.view is not None:
            base_name = self.view
            name_type = 'view'
        else:
            raise DBTableValidationError("Cannot create child: no table or view name defined")
        
        # Create new name
        new_name = f"{base_name}_{suffix}"
        
        # Build parameters for new instance
        new_params = {}
        for level in self.HIERARCHY_LEVELS:
            value = getattr(self, level)
            if value is not None:
                if level == name_type:
                    # Use canonical parameter name
                    new_params[level] = new_name
                else:
                    new_params[level] = value
        
        is_debug = False
        if is_debug:
            print("DBTable: Making Child with params of")
            pprint(new_params)

        return DBTable(**new_params)
    
    def create_child(self, suffix: str) -> 'DBTable':
        """
        Convenience method that does the same as make_child.
        
        Args:
            suffix: Suffix to append after an underscore
            
        Returns:
            New DBTable instance with modified table/view name
        """
        return self.make_child(suffix)
    
    def to_orm(self, engine, python_class_name: Optional[str] = None):
        """
        Convert this DBTable to a SQLAlchemy ORM class using reflection.
        
        Args:
            engine: SQLAlchemy engine connected to the database
            python_class_name: Optional override for the generated class name
            
        Returns:
            SQLAlchemy ORM class mapped to the table
            
        Raises:
            DBTableValidationError: If no table/view is defined
        """
        # Determine table name
        if self.table is not None:
            table_name = self.table
        elif self.view is not None:
            table_name = self.view
        else:
            raise DBTableValidationError("Cannot create ORM: no table or view name defined")
        
        # Build namespace parts for SQLAlchemy schema parameter
        namespace_parts = []
        
        # Add parts in hierarchy order (highest to lowest, excluding table/view)
        if self.catalog:
            namespace_parts.append(self.catalog)
        if self.database:
            namespace_parts.append(self.database)
        if self.schema:
            # Avoid duplicates if schema already included as database
            if self.schema not in namespace_parts:
                namespace_parts.append(self.schema)
        
        composite_schema = ".".join(namespace_parts) if namespace_parts else None
        
        # Reflect the table
        metadata_obj = MetaData()
        reflected_table = Table(
            table_name,
            metadata_obj,
            autoload_with=engine,
            schema=composite_schema
        )
        
        # Generate class name
        default_class_name = f"{table_name.title().replace('_', '').replace('-', '')}Model"
        orm_class_name = python_class_name or default_class_name
        
        # Create ORM class
        orm_class = type(
            orm_class_name,
            (DeclarativeBase,),
            {"__table__": reflected_table}
        )
        
        return orm_class
