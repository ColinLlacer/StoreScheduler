# Store Scheduler (WIP)

## Overview
A Python-based scheduling system designed to optimize employee scheduling for retail stores. This project uses advanced optimization techniques (Constraint Programming with OR-Tools) to create efficient work schedules while considering employee availability, skills, and store requirements.

Currently, the system only supports a single store at a time, and hard constraints are used for all constraints. A few constraints are still work in progress.

## Features (Work in Progress)
- Employee management with skill tracking
- Availability preferences handling
- Workload optimization
- Store-specific scheduling
- Flexible time slot management
- Role-based scheduling
- Status tracking for employees

## Database Structure
The system uses DuckDB as its database engine with the following main tables:
- **Roles**: Defines employee roles within the store
- **Status**: Tracks employee status (e.g., full-time, part-time)
- **Employees**: Stores employee information and work hour constraints
- **Skills**: Manages different skill types needed in the store
- **EmployeesSkills**: Maps employees to their skills
- **Timeslot**: Handles schedule time slots
- **Availability_Preferences**: Records employee availability preferences
- **Workload**: Defines required staffing levels for different skills and time slots
- **Store**: Stores information about retail locations

The expected schema can be found in the schema.sql file.

## Dependencies (installable with poetry)
- Python 3.10+
- OR-Tools
- Polars
- DuckDB

## Installation
TBD
