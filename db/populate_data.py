import duckdb
import logging
import datetime
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def populate_tables(db_path: str = "db/db.duckdb"):
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        # Connect to DuckDB
        con = duckdb.connect(db_path)
        logging.info(f"Connected to DuckDB at {db_path}")

        # ---------------------------
        # Populate the tables
        # ---------------------------

        # Insert data into Roles
        roles = [
            (1, 'Manager'),
            (2, 'Cashier'),
            (3, 'Stock Associate'),
            (4, 'Sales Associate'),
            (5, 'Security'),
        ]
        con.executemany("INSERT INTO Roles (RoleID, Name) VALUES (?, ?);", roles)
        logging.info("Inserted data into Roles")

        # Insert data into Status
        statuses = [
            (1, 'Full-time'),
            (2, 'Part-time'),
            (3, 'On Leave'),
            (4, 'Contract'),
        ]
        con.executemany("INSERT INTO Status (StatusID, Name) VALUES (?, ?);", statuses)
        logging.info("Inserted data into Status")

        # Insert data into Codes
        codes = [
            (1, 'Regular Shift', 'Standard working hours shift'),
            (2, 'Overtime', 'Extra hours beyond regular shift'),
            (3, 'Holiday', 'Holiday shift'),
            (4, 'Night Shift', 'Shift during night hours'),
        ]
        con.executemany("INSERT INTO Codes (CodeID, Name, Description) VALUES (?, ?, ?);", codes)
        logging.info("Inserted data into Codes")

        # Insert data into Skills
        skills = [
            (1, 'Inventory Management'),
            (2, 'Customer Service'),
            (3, 'Cash Handling'),
            (4, 'Sales'),
            (5, 'Loss Prevention'),
            (6, 'Forklift Operation'),
            (7, 'Merchandising'),
            (8, 'Cleaning'),
        ]
        con.executemany("INSERT INTO Skills (SkillID, Name) VALUES (?, ?);", skills)
        logging.info("Inserted data into Skills")

        # Insert data into Store
        stores = [
            (1,),
            (2,),
            (3,),
            (4,),
            (5,),
        ]
        con.executemany("INSERT INTO Store (StoreID) VALUES (?);", stores)
        logging.info("Inserted data into Store")

        # Insert data into Employees
        employees = [
            (1, 1, 1, 8, 4, 6, 40, 20, 30),
            (2, 2, 2, 8, 4, 5, 30, 15, 25),
            (3, 3, 2, 6, 4, 5, 25, 10, 20),
            (4, 4, 1, 8, 6, 7, 35, 25, 30),
            (5, 5, 4, 12, 8, 10, 50, 35, 45),
            (6, 2, 3, 8, 4, 6, 40, 20, 30),
            (7, 2, 1, 8, 4, 6, 40, 20, 30),
            (8, 3, 2, 6, 3, 5, 30, 15, 25),
            (9, 4, 2, 8, 5, 7, 35, 25, 30),
            (10, 5, 1, 10, 6, 8, 45, 30, 40),
        ]
        con.executemany("""
            INSERT INTO Employees (
                EmployeeID, RoleID, StatusID, DailyMaxHours, DailyMinHours, DailyOptHours,
                WeeklyMaxHours, WeeklyMinHours, WeeklyOptHours
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """, employees)
        logging.info("Inserted data into Employees")

        # Insert data into Timeslot
        timeslots = []
        timeslot_id = 1
        start_date = datetime.datetime(2023, 10, 1)
        for day_offset in range(7):  # A week of data
            date = start_date + datetime.timedelta(days=day_offset)
            for hour in range(8, 22):  # Store operates from 8 AM to 10 PM
                timeslot_datetime = date + datetime.timedelta(hours=hour)
                code_id = 1  # Regular Shift
                if date.weekday() >= 5:  # Weekend
                    code_id = 3  # Holiday shift
                timeslots.append((timeslot_id, code_id, timeslot_datetime, date.weekday(), hour))
                timeslot_id += 1
        con.executemany("""
            INSERT INTO Timeslot (
                TimeslotID, CodeID, Datetime, Day, Hour
            ) VALUES (?, ?, ?, ?, ?);
        """, timeslots)
        logging.info("Inserted data into Timeslot")

        # Insert data into EmployeesSkills
        employees_skills_data = [
            (1, 1), (1, 2), (1, 3),        # Employee 1 has skills: Inventory Management, Customer Service, Cash Handling
            (2, 2), (2, 4), (2, 3),        # Employee 2 has skills: Customer Service, Sales, Cash Handling
            (3, 1), (3, 6),                # Employee 3 has skills: Inventory Management, Forklift Operation
            (4, 2), (4, 4), (4, 7),        # Employee 4 has skills: Customer Service, Sales, Merchandising
            (5, 5), (5, 8),                # Employee 5 has skills: Loss Prevention, Cleaning
            (6, 2), (6, 3), (6, 5),        # Employee 6 has skills: Customer Service, Cash Handling, Loss Prevention
            (7, 1), (7, 4), (7, 7),        # Employee 7 has skills: Inventory Management, Sales, Merchandising
            (8, 6), (8, 8),                # Employee 8 has skills: Forklift Operation, Cleaning
            (9, 2), (9, 3), (9, 4),        # Employee 9 has skills: Customer Service, Cash Handling, Sales
            (10, 5), (10, 7), (10, 8),     # Employee 10 has skills: Loss Prevention, Merchandising, Cleaning
        ]
        con.executemany("""
            INSERT INTO EmployeesSkills (
                EmployeeID, SkillID
            ) VALUES (?, ?);
        """, employees_skills_data)
        logging.info("Inserted data into EmployeesSkills")

        # Insert data into Availability_Preferences
        availability_preferences = []
        av_pref_id = 1
        for emp in employees:
            emp_id = emp[0]
            daily_min = emp[4]
            daily_max = emp[3]
            for ts in timeslots:
                timeslot_id = ts[0]
                timeslot_hour = ts[4]
                if daily_min <= (timeslot_hour - 8) < daily_max:
                    availability_preferences.append((av_pref_id, timeslot_id, emp_id))
                    av_pref_id += 1
        con.executemany("""
            INSERT INTO Availability_Preferences (
                AvailabilitePreferencesID, TimeslotID, EmployeeID
            ) VALUES (?, ?, ?);
        """, availability_preferences)
        logging.info("Inserted data into Availability_Preferences")

        # Insert data into Workload
        workload_data = []
        unique_id = 1
        for store in stores:
            store_id = store[0]
            for ts in timeslots:
                timeslot_id = ts[0]
                hour = ts[4]
                # Define required skills and amounts based on the time of day
                if 8 <= hour < 12:
                    required_skills = [
                        (2, 2, 3),  # Customer Service
                        (3, 1, 2),  # Cash Handling
                    ]
                elif 12 <= hour < 18:
                    required_skills = [
                        (4, 2, 4),  # Sales
                        (3, 1, 2),  # Cash Handling
                    ]
                elif 18 <= hour < 22:
                    required_skills = [
                        (2, 1, 2),  # Customer Service
                        (5, 1, 1),  # Loss Prevention (Security)
                    ]
                else:
                    continue  # Store is closed
                for skill in required_skills:
                    skill_id, min_amount, opt_amount = skill
                    workload_data.append((
                        unique_id, timeslot_id, skill_id, store_id, min_amount, opt_amount
                    ))
                    unique_id += 1
        con.executemany("""
            INSERT INTO Workload (
                UniqueID, TimeslotID, SkillID, StoreID, MinAmount, OptAmount
            ) VALUES (?, ?, ?, ?, ?, ?);
        """, workload_data)
        logging.info("Inserted data into Workload")

        logging.info("All tables populated successfully.")

    except Exception as e:
        logging.error(f"An error occurred while populating tables: {e}")
    finally:
        con.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    populate_tables()