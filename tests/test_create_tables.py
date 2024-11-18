import duckdb
import pytest
from db.create_tables import create_tables
from db.populate_data import populate_tables

@pytest.fixture(scope="function")
def db_connection(tmp_path):
    """
    Pytest fixture to set up a fresh database connection for each test.
    
    Args:
        tmp_path (Path): Temporary directory provided by pytest.
        
    Yields:
        duckdb.Connection: Connection to the temporary DuckDB database.
    """
    db_path = tmp_path / "db.duckdb"
    create_tables(db_path=str(db_path))
    populate_tables(db_path=str(db_path))
    con = duckdb.connect(str(db_path))
    yield con
    con.close()

def test_create_tables(db_connection):
    """
    Test that all expected tables are created in the database.
    
    Args:
        db_connection (duckdb.Connection): Database connection fixture.
    """
    expected_tables = [
        'Roles', 'Status', 'Employees', 'Codes',
        'Timeslot', 'Availability_Preferences', 'Skills',
        'EmployeesSkills', 'Store', 'Workload'
    ]
    existing_tables = [table[0] for table in db_connection.execute("SHOW TABLES;").fetchall()]
    for table in expected_tables:
        assert table in existing_tables, f"Table {table} was not created."

def test_create_tables_columns(db_connection):
    """
    Test that each table contains the correct columns.
    
    Args:
        db_connection (duckdb.Connection): Database connection fixture.
    """
    table_columns = {
        'Roles': ['RoleID', 'Name'],
        'Status': ['StatusID', 'Name'],
        'Employees': [
            'EmployeeID', 'RoleID', 'StatusID', 'DailyMaxHours',
            'DailyMinHours', 'DailyOptHours', 'WeeklyMaxHours',
            'WeeklyMinHours', 'WeeklyOptHours'
        ],
        'Codes': ['CodeID', 'Name', 'Description'],
        'Timeslot': ['TimeslotID', 'CodeID', 'Datetime', 'Day', 'Hour'],
        'Availability_Preferences': ['AvailabilitePreferencesID', 'TimeslotID', 'EmployeeID'],
        'Skills': ['SkillID', 'Name'],
        'EmployeesSkills': ['SkillID', 'EmployeeID'],
        'Store': ['StoreID'],
        'Workload': ['UniqueID', 'TimeslotID', 'SkillID', 'StoreID', 'MinAmount', 'OptAmount']
    }

    for table, columns in table_columns.items():
        result = db_connection.execute(f"DESCRIBE {table};").fetchall()
        existing_columns = [row[0] for row in result]
        for column in columns:
            assert column in existing_columns, f"Column {column} not found in table {table}."

def test_column_data_types(db_connection):
    """
    Test that each column has the correct data type.
    
    Args:
        db_connection (duckdb.Connection): Database connection fixture.
    """
    expected_schema = {
        'Roles': {'RoleID': 'INTEGER', 'Name': 'VARCHAR'},
        'Status': {'StatusID': 'INTEGER', 'Name': 'VARCHAR'},
        'Employees': {
            'EmployeeID': 'INTEGER',
            'RoleID': 'INTEGER',
            'StatusID': 'INTEGER',
            'DailyMaxHours': 'INTEGER',
            'DailyMinHours': 'INTEGER',
            'DailyOptHours': 'INTEGER',
            'WeeklyMaxHours': 'INTEGER',
            'WeeklyMinHours': 'INTEGER',
            'WeeklyOptHours': 'INTEGER'
        },
        'Codes': {'CodeID': 'INTEGER', 'Name': 'VARCHAR', 'Description': 'VARCHAR'},
        'Timeslot': {
            'TimeslotID': 'INTEGER',
            'CodeID': 'INTEGER',
            'Datetime': 'VARCHAR',
            'Day': 'VARCHAR',
            'Hour': 'INTEGER'
        },
        'Availability_Preferences': {
            'AvailabilitePreferencesID': 'INTEGER',
            'TimeslotID': 'INTEGER',
            'EmployeeID': 'INTEGER'
        },
        'Skills': {'SkillID': 'INTEGER', 'Name': 'VARCHAR'},
        'EmployeesSkills': {'SkillID': 'INTEGER', 'EmployeeID': 'INTEGER'},
        'Store': {'StoreID': 'INTEGER'},
        'Workload': {
            'UniqueID': 'INTEGER',
            'TimeslotID': 'INTEGER',
            'SkillID': 'INTEGER',
            'StoreID': 'INTEGER',
            'MinAmount': 'INTEGER',
            'OptAmount': 'INTEGER'
        }
    }

    for table, columns in expected_schema.items():
        result = db_connection.execute(f"DESCRIBE {table};").fetchall()
        for row in result:
            column_name, column_type = row[:2]
            assert column_name in columns, f"Unexpected column {column_name} in table {table}."
            expected_type = columns[column_name].upper()
            actual_type = column_type.upper()
            assert actual_type == expected_type, (
                f"Column type mismatch in table {table} for column {column_name}: "
                f"expected {expected_type}, got {actual_type}"
            )

def test_primary_keys(db_connection):
    """
    Test that primary keys are correctly set for each table.
    
    Args:
        db_connection (duckdb.Connection): Database connection fixture.
    """
    primary_keys = {
        'Roles': ['RoleID'],
        'Status': ['StatusID'],
        'Employees': ['EmployeeID'],
        'Codes': ['CodeID'],
        'Timeslot': ['TimeslotID'],
        'Availability_Preferences': ['AvailabilitePreferencesID'],
        'Skills': ['SkillID'],
        'EmployeesSkills': ['SkillID', 'EmployeeID'],
        'Store': ['StoreID'],
        'Workload': ['UniqueID']
    }

    for table, keys in primary_keys.items():
        result = db_connection.execute(f"PRAGMA table_info('{table}');").fetchall()
        actual_keys = [row[1] for row in result if row[5] == 1]  # row[5] is pk
        assert set(keys) == set(actual_keys), (
            f"Primary key mismatch in table {table}: expected {keys}, got {actual_keys}"
        )

def test_foreign_keys(db_connection):
    """
    Test that foreign key constraints are correctly established.
    
    Args:
        db_connection (duckdb.Connection): Database connection fixture.
    """
    foreign_keys = {
        'Employees': [('RoleID', 'Roles', 'RoleID'), ('StatusID', 'Status', 'StatusID')],
        'Timeslot': [('CodeID', 'Codes', 'CodeID')],
        'Availability_Preferences': [('TimeslotID', 'Timeslot', 'TimeslotID'), ('EmployeeID', 'Employees', 'EmployeeID')],
        'EmployeesSkills': [('SkillID', 'Skills', 'SkillID'), ('EmployeeID', 'Employees', 'EmployeeID')],
        'Workload': [
            ('TimeslotID', 'Timeslot', 'TimeslotID'),
            ('SkillID', 'Skills', 'SkillID'),
            ('StoreID', 'Store', 'StoreID')
        ]
    }

    for table, fks in foreign_keys.items():
        for fk in fks:
            column, ref_table, ref_column = fk
            query = f"""
                SELECT 
                    kc.constraint_name
                FROM 
                    information_schema.key_column_usage kc
                    JOIN information_schema.table_constraints tc 
                        ON kc.constraint_name = tc.constraint_name
                WHERE 
                    tc.table_name = '{table}' 
                    AND kc.column_name = '{column}' 
                    AND tc.constraint_type = 'FOREIGN KEY';
            """
            result = db_connection.execute(query).fetchall()
            assert len(result) > 0, (
                f"Foreign key constraint on {column} in table {table} not found."
            )
            # Additional verification for referenced table and column could be implemented here