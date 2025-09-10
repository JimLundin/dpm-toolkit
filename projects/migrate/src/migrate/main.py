"""Main module for database conversion."""

from sqlite3 import Connection

from sqlalchemy import Engine, Enum, create_engine

from migrate.processing import load_data_to_database, schema_and_data


def access_to_sqlite(source_database: Engine) -> Connection:
    """Migrate Access database to SQLite.

    Args:
        source_database: Engine of the source Access database

    Returns:
        Connection to the new SQLite database

    """
    schema, tables_with_rows = schema_and_data(source_database)

    sqlite_database = create_engine("sqlite://")
    
    # DEBUG: Check what tables have Enum types before creating schema
    enum_tables = []
    for table in schema.tables.values():
        enum_columns = [(col.name, col.type) for col in table.columns if isinstance(col.type, Enum)]
        if enum_columns:
            enum_tables.append((table.name, enum_columns))
    
    if enum_tables:
        print(f"DEBUG: Creating schema with {len(enum_tables)} tables containing Enum types:")
        for table_name, enum_cols in enum_tables:
            print(f"  {table_name}: {[(name, len(typ.enums)) for name, typ in enum_cols]}")
    else:
        print("DEBUG: Creating schema with NO Enum types")
    
    schema.create_all(sqlite_database)
    
    # DEBUG: Check what constraints were actually created
    from sqlalchemy import inspect
    inspector = inspect(sqlite_database)
    
    total_created_constraints = 0
    for table_name in schema.tables.keys():
        try:
            constraints = inspector.get_check_constraints(table_name)
            if constraints:
                total_created_constraints += len(constraints)
                print(f"DEBUG: {table_name} created {len(constraints)} constraints")
        except:
            pass
    
    print(f"DEBUG: Total CHECK constraints created in SQLite: {total_created_constraints}")
    
    load_data_to_database(sqlite_database, tables_with_rows)

    return sqlite_database.raw_connection().connection
