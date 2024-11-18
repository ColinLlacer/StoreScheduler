import duckdb
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_tables(db_path: str = "db/db.duckdb"):
    """
    Creates all necessary tables in the DuckDB database.

    Args:
        db_path (str): The file path to the DuckDB database. Defaults to "db/db.duckdb".
    """
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # Connect to DuckDB
        con = duckdb.connect(db_path)
        logging.info(f"Connected to DuckDB at {db_path}")

        # Create Roles table
        con.execute("""
            CREATE TABLE IF NOT EXISTS Roles (
                RoleID INTEGER PRIMARY KEY,
                Name TEXT
            );
        """)
        logging.info("Created table: Roles")

        # Create Status table
        con.execute("""
            CREATE TABLE IF NOT EXISTS Status (
                StatusID INTEGER PRIMARY KEY,
                Name TEXT
            );
        """)
        logging.info("Created table: Status")

        # Create Employees table
        con.execute("""
            CREATE TABLE IF NOT EXISTS Employees (
                EmployeeID INTEGER PRIMARY KEY,
                RoleID INTEGER,
                StatusID INTEGER,
                DailyMaxHours INTEGER,
                DailyMinHours INTEGER,
                DailyOptHours INTEGER,
                WeeklyMaxHours INTEGER,
                WeeklyMinHours INTEGER,
                WeeklyOptHours INTEGER,
                FOREIGN KEY (RoleID) REFERENCES Roles(RoleID),
                FOREIGN KEY (StatusID) REFERENCES Status(StatusID)
            );
        """)
        logging.info("Created table: Employees")

        # Create Codes table
        con.execute("""
            CREATE TABLE IF NOT EXISTS Codes (
                CodeID INTEGER PRIMARY KEY,
                Name TEXT,
                Description TEXT
            );
        """)
        logging.info("Created table: Codes")

        # Create Timeslot table
        con.execute("""
            CREATE TABLE IF NOT EXISTS Timeslot (
                TimeslotID INTEGER PRIMARY KEY,
                CodeID INTEGER,
                Datetime TEXT,
                Day TEXT,
                Hour INTEGER,
                FOREIGN KEY (CodeID) REFERENCES Codes(CodeID)
            );
        """)
        logging.info("Created table: Timeslot")

        # Create Availability_Preferences table
        con.execute("""
            CREATE TABLE IF NOT EXISTS Availability_Preferences (
                AvailabilitePreferencesID INTEGER PRIMARY KEY,
                TimeslotID INTEGER,
                EmployeeID INTEGER,
                FOREIGN KEY (TimeslotID) REFERENCES Timeslot(TimeslotID),
                FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID)
            );
        """)
        logging.info("Created table: Availability_Preferences")

        # Create Skills table
        con.execute("""
            CREATE TABLE IF NOT EXISTS Skills (
                SkillID INTEGER PRIMARY KEY,
                Name TEXT
            );
        """)
        logging.info("Created table: Skills")

        # Create EmployeesSkills table
        con.execute("""
            CREATE TABLE IF NOT EXISTS EmployeesSkills (
                SkillID INTEGER,
                EmployeeID INTEGER,
                PRIMARY KEY (SkillID, EmployeeID),
                FOREIGN KEY (SkillID) REFERENCES Skills(SkillID),
                FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID)
            );
        """)
        logging.info("Created table: EmployeesSkills")

        # Create Store table
        con.execute("""
            CREATE TABLE IF NOT EXISTS Store (
                StoreID INTEGER PRIMARY KEY
                -- Add additional fields as needed
            );
        """)
        logging.info("Created table: Store")

        # Create Workload table
        con.execute("""
            CREATE TABLE IF NOT EXISTS Workload (
                UniqueID INTEGER PRIMARY KEY,
                TimeslotID INTEGER,
                SkillID INTEGER,
                StoreID INTEGER,
                MinAmount INTEGER,    -- Minimum number of employees needed
                OptAmount INTEGER,    -- Optimal number of employees needed
                FOREIGN KEY (TimeslotID) REFERENCES Timeslot(TimeslotID),
                FOREIGN KEY (SkillID) REFERENCES Skills(SkillID),
                FOREIGN KEY (StoreID) REFERENCES Store(StoreID)
            ); 
        """)
        logging.info("Created table: Workload")

    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        raise
    finally:
        con.close()
        logging.info("Closed DuckDB connection.")

if __name__ == "__main__":
    create_tables()