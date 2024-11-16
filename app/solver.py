import logging
import traceback
from ortools.sat.python import cp_model
import duckdb
import polars as pl
from app.config import (
    DATABASE_PATH,
    LOG_LEVEL,
    LOG_FORMAT,
    HOURS_BLOCKS,
    ENABLE_WORK_INDICATOR,
    ENABLE_TRANSITION,
    ENABLE_CONSECUTIVE_DAYS_OFF,
    ENABLE_HOURS,
    ENABLE_WORKLOAD,
)

# Configure logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)


def add_work_indicator_constraints(model, timeslots_vars, work_e_d, num_employees, num_days, hours_blocks):
    """
    Adds constraints to link work indicators with shift variables.

    Args:
        model (cp_model.CpModel): The CP model.
        timeslots_vars (dict): Dictionary of shift variables.
        work_e_d (dict): Dictionary of work indicator variables.
        num_employees (int): Number of employees.
        num_days (int): Number of days.
        hours_blocks (int): Number of hourly blocks per day.
    """
    logging.info("Adding work indicator constraints.")
    for e in range(num_employees):
        work_e_d[e] = {}
        for d in range(num_days):
            work_var = model.NewBoolVar(f"work_e{e}_d{d}")
            work_e_d[e][d] = work_var

            # Sum of shifts for the day
            shift_sum = sum(timeslots_vars[(e, d, h)] for h in range(hours_blocks))

            # Link the work indicator to the sum of shifts
            model.Add(shift_sum > 0).OnlyEnforceIf(work_var)
            model.Add(shift_sum == 0).OnlyEnforceIf(work_var.Not())
    logging.info("Work indicator constraints added successfully.")


def add_transition_constraints(model, timeslots_vars, num_employees, num_days, hours_blocks):
    """
    Adds transition constraints to ensure consecutive timeslots assignments.

    Args:
        model (cp_model.CpModel): The CP model.
        timeslots_vars (dict): Dictionary of shift variables.
        num_employees (int): Number of employees.
        num_days (int): Number of days.
        hours_blocks (int): Number of hourly blocks per day.
    """
    logging.info("Adding transition constraints.")
    for e in range(num_employees):
        for d in range(num_days):
            transition_vars = []
            for h in range(hours_blocks - 1):
                transition = model.NewBoolVar(f"transition_e{e}_d{d}_h{h}")
                transition_vars.append(transition)

                # Transition is 1 if there's a change between h and h+1
                model.Add(timeslots_vars[(e, d, h)] != timeslots_vars[(e, d, h + 1)]).OnlyEnforceIf(transition)
                model.Add(timeslots_vars[(e, d, h)] == timeslots_vars[(e, d, h + 1)]).OnlyEnforceIf(transition.Not())

            # Limit the number of transitions to ensure consecutive assignments
            model.Add(sum(transition_vars) <= 2)
    logging.info("Transition constraints added successfully.")


def add_consecutive_days_off_constraints(model, work_e_d, num_employees, num_days):
    """
    Adds constraints to ensure at least two consecutive days off for each employee.

    Args:
        model (cp_model.CpModel): The CP model.
        work_e_d (dict): Dictionary of work indicator variables.
        num_employees (int): Number of employees.
        num_days (int): Number of days.
    """
    logging.info("Adding consecutive days off constraints.")
    for e in range(num_employees):
        consecutive_days_off = []
        for d in range(num_days - 1):
            both_off = model.NewBoolVar(f"both_off_e{e}_d{d}_d{d+1}")
            consecutive_days_off.append(both_off)

            # both_off is true if both days are off
            model.Add(work_e_d[e][d] == 0).OnlyEnforceIf(both_off)
            model.Add(work_e_d[e][d + 1] == 0).OnlyEnforceIf(both_off)

        # Ensure at least one pair of consecutive days off
        model.Add(sum(consecutive_days_off) >= 1)
    logging.info("Consecutive days off constraints added successfully.")


def add_hour_constraints(model, timeslots_vars, employees_df, num_employees, num_days, hours_blocks):
    """
    Adds constraints for minimum and maximum working hours per day and week.

    Args:
        model (cp_model.CpModel): The CP model.
        timeslots_vars (dict): Dictionary of shift variables.
        employees_df (pl.DataFrame): DataFrame containing employee constraints.
        num_employees (int): Number of employees.
        num_days (int): Number of days.
        hours_blocks (int): Number of hourly blocks per day.
    """
    logging.info("Adding hour constraints (daily and weekly).")
    for e in range(num_employees):
        try:
            employee_row = employees_df.row(e)
            logging.debug(f"Employee Row {e}: {employee_row}")

            # Access using attribute access if available
            if hasattr(employee_row, 'WeeklyMinHours') and hasattr(employee_row, 'WeeklyMaxHours') and hasattr(employee_row, 'WeeklyOptHours'):
                weekly_min = employee_row.WeeklyMinHours
                weekly_max = employee_row.WeeklyMaxHours
                weekly_opt = employee_row.WeeklyOptHours
            else:
                # Fallback to integer indices based on schema
                # Adjust the indices based on the actual order of columns in your DataFrame
                weekly_min = employee_row[employees_df.columns.index("WeeklyMinHours")]
                weekly_max = employee_row[employees_df.columns.index("WeeklyMaxHours")]
                weekly_opt = employee_row[employees_df.columns.index("WeeklyOptHours")]

            logging.debug(f"Employee {e}: WeeklyMin={weekly_min}, WeeklyMax={weekly_max}, WeeklyOpt={weekly_opt}")

            # Aggregate weekly worked hours
            weekly_worked_hours = [timeslots_vars[(e, d, h)] for d in range(num_days) for h in range(hours_blocks)]

            # Weekly constraints
            model.Add(sum(weekly_worked_hours) >= weekly_min)
            model.Add(sum(weekly_worked_hours) <= weekly_max)

            for d in range(num_days):
                # Access daily constraints similarly
                if hasattr(employee_row, 'DailyMinHours') and hasattr(employee_row, 'DailyMaxHours') and hasattr(employee_row, 'DailyOptHours'):
                    daily_min = employee_row.DailyMinHours
                    daily_max = employee_row.DailyMaxHours
                    daily_opt = employee_row.DailyOptHours
                else:
                    daily_min = employee_row[employees_df.columns.index("DailyMinHours")]
                    daily_max = employee_row[employees_df.columns.index("DailyMaxHours")]
                    daily_opt = employee_row[employees_df.columns.index("DailyOptHours")]

                logging.debug(f"Employee {e}, Day {d}: DailyMin={daily_min}, DailyMax={daily_max}, DailyOpt={daily_opt}")

                # Aggregate daily worked hours
                daily_worked_hours = [timeslots_vars[(e, d, h)] for h in range(hours_blocks)]

                # Daily constraints
                model.Add(sum(daily_worked_hours) >= daily_min)
                model.Add(sum(daily_worked_hours) <= daily_max)
        except IndexError:
            logging.error(f"Employee index {e} is out of range.")
            continue
        except AttributeError as attr_err:
            logging.error(f"Attribute error accessing employee row {e}: {attr_err}")
            continue
        except KeyError as key_err:
            logging.error(f"Key error accessing employee row {e}: {key_err}")
            continue
        except Exception as e:
            logging.error(f"Unexpected error in add_hour_constraints for employee {e}: {e}")
            continue
    logging.info("Hour constraints added successfully.")

# TO DO: add optimal workload constraints
def add_workload_constraints(model, workload_dict, skill_to_employees, timeslots_vars, timeslots_df, skills, num_timeslots, num_days, hours_blocks, manager_employee_ids):
    """
    Adds constraints to ensure workload by skill for each timeslot is respected.

    Args:
        model (cp_model.CpModel): The CP model.
        workload_dict (dict): Dictionary mapping (TimeslotID, SkillID) to workload requirements.
        skill_to_employees (dict): Dictionary mapping SkillID to list of EmployeeIDs.
        timeslots_vars (dict): Dictionary of shift variables.
        timeslots_df (pl.DataFrame): DataFrame containing timeslot information.
        skills (list): List of SkillIDs.
        num_timeslots (int): Number of timeslots.
        num_days (int): Number of days.
        hours_blocks (int): Number of hourly blocks per day.
        manager_employee_ids (list): List of EmployeeIDs who are managers.
    """
    logging.info("Adding workload constraints.")
    for ts in range(1, num_timeslots + 1):
        try:
            timeslot_row = timeslots_df.filter(pl.col('TimeslotID') == ts).row(0)
            logging.debug(f"Timeslot Row for ts={ts}: {timeslot_row}")

            # Access using attribute access if available
            if hasattr(timeslot_row, 'Day') and hasattr(timeslot_row, 'Hour'):
                day = timeslot_row.Day
                hour = timeslot_row.Hour
            else:
                # Fallback to integer indices based on schema
                day = timeslot_row[timeslots_df.columns.index("Day")]
                hour = timeslot_row[timeslots_df.columns.index("Hour")]

            logging.debug(f"Timeslot ts={ts}: day={day}, hour={hour}")

        except IndexError:
            logging.error(f"TimeslotID {ts} not found in Timeslots dataframe.")
            continue
        except AttributeError as attr_err:
            logging.error(f"Attribute error accessing timeslot_row for ts={ts}: {attr_err}")
            continue
        except KeyError as key_err:
            logging.error(f"Key error accessing timeslot_row for ts={ts}: {key_err}")
            continue
        except Exception as e:
            logging.error(f"Unexpected error accessing timeslot_row for ts={ts}: {e}")
            continue

        for skill in skills:
            workload = workload_dict.get((ts, skill))
            if workload is None:
                logging.debug(f"No workload defined for TimeslotID {ts} and SkillID {skill}. Skipping constraint.")
                continue

            min_workload = workload.get('MinAmount', 0)
            opt_workload = workload.get('OptAmount', 0)
            eligible_employees = skill_to_employees.get(skill, [])

            if not eligible_employees:
                logging.warning(f"No employees found with SkillID {skill} for TimeslotID {ts}.")
                if min_workload > 0:
                    raise ValueError(
                        f"No eligible employees for SkillID {skill} at TimeslotID {ts}, "
                        f"but minimum workload is {min_workload}."
                    )
                continue

            assigned_vars = [
                timeslots_vars.get((e, day, hour)) 
                for e in eligible_employees 
                if (e, day, hour) in timeslots_vars
            ]

            if not assigned_vars:
                logging.warning(f"No shift variables found for eligible employees with SkillID {skill} at TimeslotID {ts}.")
                continue

            # Add constraints to meet the minimum workload
            model.Add(sum(assigned_vars) >= min_workload)

            # TODO: Add constraints for optimal workload if necessary

        # Constraint: An employee should not be booked multiple times in the same timeslot
        for e in manager_employee_ids:
            employee_assignments = [
                timeslots_vars.get((e, day, hour)) 
                for skill in skills 
                if (e, day, hour) in timeslots_vars
            ]

            if employee_assignments:
                # Ensure the employee is assigned to at most one skill in this timeslot
                model.Add(sum(employee_assignments) <= 1)

            # Manager assignment constraints
            for skill in skills:
                if workload_dict.get((ts, skill), {}).get('MinAmount', 0) > 0:
                    manager_assignments = [
                        timeslots_vars.get((e, day, hour))
                        for e in manager_employee_ids
                        if (e, day, hour) in timeslots_vars
                    ]
                    if manager_assignments:
                        model.Add(sum(manager_assignments) >= 1)
                    else:
                        logging.warning(f"No valid manager assignments found for TimeslotID {ts}, Day {day}, Hour {hour}.")
    logging.info("Workload constraints added successfully.")


def setup_constraints(model, timeslots_vars, work_e_d, employees_df, workload_dict, skill_to_employees, timeslots_df, skills, num_employees, num_days, hours_blocks, num_timeslots, manager_employee_ids, enable_work_indicator=True, enable_transition=True, enable_consecutive_days_off=True, enable_hours=True, enable_workload=True):
    """
    Sets up all constraints based on the provided flags.

    Args:
        model (cp_model.CpModel): The CP model.
        timeslots_vars (dict): Dictionary of shift variables.
        work_e_d (dict): Dictionary of work indicator variables.
        employees_df (pl.DataFrame): DataFrame containing employee constraints.
        workload_dict (dict): Dictionary mapping (TimeslotID, SkillID) to workload requirements.
        skill_to_employees (dict): Dictionary mapping SkillID to list of EmployeeIDs.
        timeslots_df (pl.DataFrame): DataFrame containing timeslot information.
        skills (list): List of SkillIDs.
        num_employees (int): Number of employees.
        num_days (int): Number of days.
        hours_blocks (int): Number of hourly blocks per day.
        num_timeslots (int): Number of timeslots.
        manager_employee_ids (list): List of EmployeeIDs who are managers.
        enable_work_indicator (bool): Flag to enable/disable work indicator constraints.
        enable_transition (bool): Flag to enable/disable transition constraints.
        enable_consecutive_days_off (bool): Flag to enable/disable consecutive days off constraints.
        enable_hours (bool): Flag to enable/disable hour constraints.
        enable_workload (bool): Flag to enable/disable workload constraints.
    """
    logging.info("Setting up constraints.")

    if enable_work_indicator:
        add_work_indicator_constraints(model, timeslots_vars, work_e_d, num_employees, num_days, hours_blocks)

    if enable_transition:
        add_transition_constraints(model, timeslots_vars, num_employees, num_days, hours_blocks)

    if enable_consecutive_days_off:
        add_consecutive_days_off_constraints(model, work_e_d, num_employees, num_days)

    if enable_hours:
        add_hour_constraints(model, timeslots_vars, employees_df, num_employees, num_days, hours_blocks)

    if enable_workload:
        add_workload_constraints(
            model, workload_dict, skill_to_employees, timeslots_vars,
            timeslots_df, skills, num_timeslots, num_days, hours_blocks, manager_employee_ids
        )

    logging.info("All enabled constraints have been set up.")


def main():
    """
    Main function to set up and solve the CP model.
    """
    try:
        # Connect to the database
        con = duckdb.connect(DATABASE_PATH)
        logging.info("Connected to DuckDB database.")

        model = cp_model.CpModel()

        # Fetch data
        employees = con.execute("SELECT * FROM Employees").fetchdf()
        employees_df = pl.from_pandas(employees)

        timeslots = con.execute("SELECT * FROM Timeslot").fetchdf()
        timeslots_df = pl.from_pandas(timeslots)

        employees_skills = con.execute("SELECT * FROM EmployeesSkills").fetchdf()
        employees_skills_df = pl.from_pandas(employees_skills)

        workload = con.execute("SELECT * FROM Workload").fetchdf()
        workload_df = pl.from_pandas(workload)

        num_employees = employees_df.height
        hours_blocks = HOURS_BLOCKS
        num_days = con.execute("SELECT COUNT(DISTINCT CAST(Datetime AS DATE)) as days FROM Timeslot").fetchone()[0]
        num_timeslots = timeslots_df.height
        skills_query = con.execute("SELECT DISTINCT SkillID FROM Workload")
        skills = [row[0] for row in skills_query.fetchall()]

        manager_employee_ids = con.execute("SELECT EmployeeID FROM Employees WHERE RoleID = 1").fetchall()
        manager_employee_ids = [item[0] for item in manager_employee_ids]

        # Initialize dictionaries to store variables
        timeslots_vars = {}
        work_e_d = {}

        for e in range(num_employees):
            for d in range(num_days):
                for h in range(hours_blocks):
                    var_name = f"shift_e{e}_d{d}_h{h}"
                    timeslots_vars[(e, d, h)] = model.NewBoolVar(var_name)

        # Preprocess workload data and skills to employee mapping into dictionaries for quick access
        workload_dict = {
            (row['TimeslotID'], row['SkillID']): {'MinAmount': row['MinAmount'], 'OptAmount': row['OptAmount']}
            for row in workload_df.iter_rows(named=True)
        }
        skill_to_employees = {
            skill: employees_skills_df.filter(pl.col('SkillID') == skill)
                                       .select('EmployeeID')
                                       .to_series()
                                       .to_list()
            for skill in skills
        }

        # Set up constraints
        setup_constraints(
            model, timeslots_vars, work_e_d, employees_df, workload_dict,
            skill_to_employees, timeslots_df, skills, num_employees,
            num_days, hours_blocks, num_timeslots, manager_employee_ids,
            enable_work_indicator=ENABLE_WORK_INDICATOR,
            enable_transition=ENABLE_TRANSITION,
            enable_consecutive_days_off=ENABLE_CONSECUTIVE_DAYS_OFF,
            enable_hours=ENABLE_HOURS,
            enable_workload=ENABLE_WORKLOAD
        )

        logging.info("Constraint setup completed successfully.")

        # Create a solver and solve the model
        solver = cp_model.CpSolver()
        logging.info("Starting to solve the model...")
        status = solver.Solve(model)

        # Process the solution
        if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
            logging.info(f"Solution found with status: {solver.StatusName(status)}")
            
            # Print solution details
            for e in range(num_employees):
                for d in range(num_days):
                    shifts = []
                    for h in range(hours_blocks):
                        if solver.Value(timeslots_vars[(e, d, h)]) == 1:
                            shifts.append(h)
                    if shifts:
                        logging.info(f"Employee {e} works on day {d} during hours: {shifts}")
            
            logging.info(f"Objective value: {solver.ObjectiveValue()}")
            logging.info(f"Wall time: {solver.WallTime()} seconds")
        else:
            logging.error(f"No solution found. Status: {solver.StatusName(status)}")

    except Exception as e:
        logging.error(f"An error occurred in the solver setup: {e}")
        logging.error(traceback.format_exc())
    finally:
        con.close()
        logging.info("Database connection closed.")


if __name__ == "__main__":
    main()

