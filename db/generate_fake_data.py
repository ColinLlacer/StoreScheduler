import duckdb
import logging
from datetime import datetime, timedelta
import random

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_fake_data():
    try:
        # Connect to DuckDB
        con = duckdb.connect("db.duckdb")
        logging.info("Connected to the database successfully.")

        # Generate Roles data
        roles = [
            (1, "Manager"),
            (2, "Senior Employee"),
            (3, "Junior Employee")
        ]
        con.executemany("INSERT INTO Roles (RoleID, Name) VALUES (?, ?)", roles)
        logging.info("Inserted fake data into Roles table")

        # Generate Status data
        statuses = [
            (1, "Full-time"),
            (2, "Part-time"),
            (3, "Temporary")
        ]
        con.executemany("INSERT INTO Status (StatusID, Name) VALUES (?, ?)", statuses)
        logging.info("Inserted fake data into Status table")

        # Generate Skills data
        skills = [
            (1, "Cashier"),
            (2, "Stock Management"),
            (3, "Customer Service"),
            (4, "Department Specialist")
        ]
        con.executemany("INSERT INTO Skills (SkillID, Name) VALUES (?, ?)", skills)
        logging.info("Inserted fake data into Skills table")

        # Generate Store data
        stores = [(1,), (2,)]  # Two stores
        con.executemany("INSERT INTO Store (StoreID) VALUES (?)", stores)
        logging.info("Inserted fake data into Store table")

        # Generate Employees data
        employees = []
        for i in range(1, 11):  # 10 employees
            employees.append((
                i,  # EmployeeID
                random.randint(1, 3),  # RoleID
                random.randint(1, 3),  # StatusID
                8,  # DailyMaxHours
                4,  # DailyMinHours
                6,  # DailyOptHours
                40,  # WeeklyMaxHours
                20,  # WeeklyMinHours
                32,  # WeeklyOptHours
            ))
        con.executemany("""
            INSERT INTO Employees 
            (EmployeeID, RoleID, StatusID, DailyMaxHours, DailyMinHours, DailyOptHours, 
             WeeklyMaxHours, WeeklyMinHours, WeeklyOptHours) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, employees)
        logging.info("Inserted fake data into Employees table")

        # Generate EmployeesSkills data
        employees_skills = []
        for emp_id in range(1, 11):
            # Each employee gets 2-3 random skills
            num_skills = random.randint(2, 3)
            skill_ids = random.sample(range(1, 5), num_skills)
            for skill_id in skill_ids:
                employees_skills.append((skill_id, emp_id))
        con.executemany("INSERT INTO EmployeesSkills (SkillID, EmployeeID) VALUES (?, ?)", employees_skills)
        logging.info("Inserted fake data into EmployeesSkills table")

        # Generate Codes data (for different types of time slots)
        codes = [
            (1, "Regular", "Regular working hours"),
            (2, "Holiday", "Holiday working hours"),
            (3, "Special", "Special event hours")
        ]
        con.executemany("INSERT INTO Codes (CodeID, Name, Description) VALUES (?, ?, ?)", codes)
        logging.info("Inserted fake data into Codes table")

        # Generate Timeslot data for the next 7 days
        timeslots = []
        start_date = datetime.now().replace(hour=8, minute=0, second=0, microsecond=0)
        slot_id = 1
        for day in range(7):  # 7 days
            current_date = start_date + timedelta(days=day)
            for hour in range(8, 20, 2):  # 2-hour slots from 8 AM to 8 PM
                timeslots.append((
                    slot_id,
                    1,  # CodeID (Regular)
                    current_date.replace(hour=hour)
                ))
                slot_id += 1
        con.executemany("INSERT INTO Timeslot (TimeslotID, CodeID, Datetime) VALUES (?, ?, ?)", timeslots)
        logging.info("Inserted fake data into Timeslot table")

        # Generate Workload data
        workload = []
        unique_id = 1
        for slot_id in range(1, len(timeslots) + 1):
            for skill_id in range(1, 5):  # For each skill
                for store_id in range(1, 3):  # For each store
                    workload.append((
                        unique_id,
                        slot_id,
                        skill_id,
                        store_id,
                        1,  # MinAmount
                        2,  # OptAmount
                    ))
                    unique_id += 1
        con.executemany("""
            INSERT INTO Workload 
            (UniqueID, TimeslotID, SkillID, StoreID, MinAmount, OptAmount) 
            VALUES (?, ?, ?, ?, ?, ?)
        """, workload)
        logging.info("Inserted fake data into Workload table")

        # Generate Availability_Preferences data
        availability_prefs = []
        pref_id = 1
        for emp_id in range(1, 11):
            # Each employee gets random availability preferences
            for slot_id in random.sample(range(1, len(timeslots) + 1), 20):  # 20 random slots per employee
                availability_prefs.append((
                    pref_id,
                    slot_id,
                    emp_id
                ))
                pref_id += 1
        con.executemany("""
            INSERT INTO Availability_Preferences 
            (AvailabilitePreferencesID, TimeslotID, EmployeeID) 
            VALUES (?, ?, ?)
        """, availability_prefs)
        logging.info("Inserted fake data into Availability_Preferences table")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        con.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    generate_fake_data()
