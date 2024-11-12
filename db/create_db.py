import duckdb
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_tables():
    try:
        # Connect to DuckDB
        con = duckdb.connect("db.duckdb")
        logging.info("Connected to the database successfully.")

        # Drop tables if they exist in reverse order of dependencies
        con.execute("DROP TABLE IF EXISTS Workload;")
        con.execute("DROP TABLE IF EXISTS Availability_Preferences;")
        con.execute("DROP TABLE IF EXISTS EmployeesSkills;")
        con.execute("DROP TABLE IF EXISTS Timeslot;")
        con.execute("DROP TABLE IF EXISTS Employees;")
        con.execute("DROP TABLE IF EXISTS Codes;")
        con.execute("DROP TABLE IF EXISTS Skills;")
        con.execute("DROP TABLE IF EXISTS Store;")
        con.execute("DROP TABLE IF EXISTS Roles;")
        con.execute("DROP TABLE IF EXISTS Status;")

        # SQL statements to create tables
        con.execute("""
            CREATE TABLE Roles (
                RoleID INTEGER PRIMARY KEY,
                Name TEXT
            );
        """)
        logging.info("Created table: Roles")

        con.execute("""
            CREATE TABLE Status (
                StatusID INTEGER PRIMARY KEY,
                Name TEXT
            );
        """)
        logging.info("Created table: Status")

        con.execute("""
            CREATE TABLE Employees (
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

        con.execute("""
            CREATE TABLE Codes (
                CodeID INTEGER PRIMARY KEY,
                Name TEXT,
                Description TEXT
            );
        """)
        logging.info("Created table: Codes")

        con.execute("""
            CREATE TABLE Timeslot (
                TimeslotID INTEGER PRIMARY KEY,
                CodeID INTEGER,
                Datetime TIMESTAMP,
                Day INTEGER,
                Hour INTEGER,
                FOREIGN KEY (CodeID) REFERENCES Codes(CodeID)
            );
        """)
        logging.info("Created table: Timeslot")

        con.execute("""
            CREATE TABLE Availability_Preferences (
                AvailabilitePreferencesID INTEGER PRIMARY KEY,
                TimeslotID INTEGER,
                EmployeeID INTEGER,
                -- Add additional fields as needed
                FOREIGN KEY (TimeslotID) REFERENCES Timeslot(TimeslotID),
                FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID)
            );
        """)
        logging.info("Created table: Availability_Preferences")

        con.execute("""
            CREATE TABLE Skills (
                SkillID INTEGER PRIMARY KEY,
                Name TEXT
            );
        """)
        logging.info("Created table: Skills")

        con.execute("""
            CREATE TABLE EmployeesSkills (
                SkillID INTEGER,
                EmployeeID INTEGER,
                PRIMARY KEY (SkillID, EmployeeID),
                FOREIGN KEY (SkillID) REFERENCES Skills(SkillID),
                FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID)
            );
        """)
        logging.info("Created table: EmployeesSkills")

        con.execute("""
            CREATE TABLE Store (
                StoreID INTEGER PRIMARY KEY
                -- Add additional fields as needed
            );
        """)
        logging.info("Created table: Store")

        con.execute("""
            CREATE TABLE Workload (
                UniqueID INTEGER PRIMARY KEY,
                TimeslotID INTEGER,
                SkillID INTEGER,
                StoreID INTEGER,
                MinAmount INTEGER,
                OptAmount INTEGER,
                FOREIGN KEY (TimeslotID) REFERENCES Timeslot(TimeslotID),
                FOREIGN KEY (SkillID) REFERENCES Skills(SkillID),
                FOREIGN KEY (StoreID) REFERENCES Store(StoreID)
            );
        """)
        logging.info("Created table: Workload")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        con.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    create_tables()