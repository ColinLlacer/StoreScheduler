-- Database Schema for Employee Scheduling System

-- Roles: Stores different roles/positions in the organization
CREATE TABLE Roles (
    RoleID INTEGER PRIMARY KEY,
    Name TEXT
);

-- Status: Stores employee status types (e.g., Full-time, Part-time, etc.)
CREATE TABLE Status (
    StatusID INTEGER PRIMARY KEY,
    Name TEXT
);

-- Employees: Main employee information including work hour constraints
CREATE TABLE Employees (
    EmployeeID INTEGER PRIMARY KEY,
    RoleID INTEGER,
    StatusID INTEGER,
    DailyMaxHours INTEGER,  -- Maximum hours an employee can work per day
    DailyMinHours INTEGER,  -- Minimum hours an employee must work per day
    DailyOptHours INTEGER,  -- Optimal/preferred hours per day
    WeeklyMaxHours INTEGER, -- Maximum hours an employee can work per week
    WeeklyMinHours INTEGER, -- Minimum hours an employee must work per week
    WeeklyOptHours INTEGER, -- Optimal/preferred hours per week
    FOREIGN KEY (RoleID) REFERENCES Roles(RoleID),
    FOREIGN KEY (StatusID) REFERENCES Status(StatusID)
);

-- Codes: Reference table for different time codes (e.g., regular hours, overtime, etc.)
CREATE TABLE Codes (
    CodeID INTEGER PRIMARY KEY,
    Name TEXT,
    Description TEXT
);

-- Timeslot: Defines specific time periods
CREATE TABLE Timeslot (
    TimeslotID INTEGER PRIMARY KEY,
    CodeID INTEGER,
    Datetime TIMESTAMP,
    Day INTEGER,      -- Day of the week
    Hour INTEGER,     -- Hour of the day
    FOREIGN KEY (CodeID) REFERENCES Codes(CodeID)
);

-- Availability_Preferences: Stores employee availability and preferences for specific timeslots
CREATE TABLE Availability_Preferences (
    AvailabilitePreferencesID INTEGER PRIMARY KEY,
    TimeslotID INTEGER,
    EmployeeID INTEGER,
    FOREIGN KEY (TimeslotID) REFERENCES Timeslot(TimeslotID),
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID)
);

-- Skills: Catalog of different skills required for various tasks
CREATE TABLE Skills (
    SkillID INTEGER PRIMARY KEY,
    Name TEXT
);

-- EmployeesSkills: Many-to-many relationship between employees and their skills
CREATE TABLE EmployeesSkills (
    SkillID INTEGER,
    EmployeeID INTEGER,
    PRIMARY KEY (SkillID, EmployeeID),
    FOREIGN KEY (SkillID) REFERENCES Skills(SkillID),
    FOREIGN KEY (EmployeeID) REFERENCES Employees(EmployeeID)
);

-- Store: Store information
CREATE TABLE Store (
    StoreID INTEGER PRIMARY KEY
    -- Additional fields can be added as needed
);

-- Workload: Defines required staffing levels for different skills at specific times
CREATE TABLE Workload (
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