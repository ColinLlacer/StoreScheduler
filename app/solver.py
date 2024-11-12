from ortools.sat.python import cp_model
import duckdb
import polars as pl
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Connect to the database
con = duckdb.connect("db.duckdb")
model = cp_model.CpModel()

# Fetch data
employees = con.execute("SELECT COUNT(*) FROM Employees").fetchdf()
employees_df = pl.from_pandas(employees)

timeslots = con.execute("SELECT * FROM Timeslot").fetchall()
timeslots_df = pl.from_pandas(timeslots)

employees_skills = con.execute("SELECT * FROM EmployeesSkills").fetchall()
employees_skills_df = pl.from_pandas(employees_skills)

workload = con.execute("SELECT * FROM Workload").fetchall()
workload_df = pl.from_pandas(workload)

num_employees = employees_df.height
hours_blocks = 24
num_days = con.execute("SELECT COUNT(DISTINCT DATE(Datetime)) as days FROM Timeslot").fetchone()[0]
num_timeslots = timeslots_df.height
skills = con.execute("SELECT DISTINCT SkillID FROM Workload").fetchall()


# Initialize dictionaries to store variables
timeslots_vars = {}
work_e_d = {}

for e in range(num_employees):
    work_e_d[e] = {}
    for d in range(num_days):
        # Create Boolean variables for each hour
        for h in range(hours_blocks):
            var_name = f"shift_e{e}_d{d}_h{h}"
            timeslots_vars[(e, d, h)] = model.NewBoolVar(var_name)
        
        # Create and link work indicator variable
        work_var = model.NewBoolVar(f"work_e{e}_d{d}")
        work_e_d[e][d] = work_var
        
        # Link the work indicator to the shift variables
        # If the sum of shifts > 0, then work_var = 1
        model.Add(sum(timeslots_vars[(e, d, h)] for h in range(hours_blocks)) > 0).OnlyEnforceIf(work_var)
        # If the sum of shifts == 0, then work_var = 0
        model.Add(sum(timeslots_vars[(e, d, h)] for h in range(hours_blocks)) == 0).OnlyEnforceIf(work_var.Not())

# Add transition constraints to ensure that timeslots assigned to an employee are consecutive
for e in range(num_employees):
    for d in range(num_days):
        transition_vars = []
        for h in range(hours_blocks - 1):
            # Create a transition variable: 1 if there's a change between h and h+1
            transition = model.NewBoolVar(f"transition_e{e}_d{d}_h{h}")
            transition_vars.append(transition)
            
            # If current hour is different from the next hour, transition is 1
            model.Add(timeslots_vars[(e, d, h)] != timeslots_vars[(e, d, h + 1)]).OnlyEnforceIf(transition)
            # If current hour is same as the next hour, transition is 0
            model.Add(timeslots_vars[(e, d, h)] == timeslots_vars[(e, d, h + 1)]).OnlyEnforceIf(transition.Not())
        
        # The sum of transitions should be <= 2 (one start and one end)
        model.Add(sum(transition_vars) <= 2)



# Check minimum and maximum hours constraints (daily and weekly)
# OPTIMAL NUMBER OF HOURS TO BE ADDED HERE
for e in range(num_employees):
    # Retrieve employee-specific constraints from the DataFrame
    weekly_min = employees_df.row(e)["WeeklyMinHours"]
    weekly_max = employees_df.row(e)["WeeklyMaxHours"] 
    weekly_opt = employees_df.row(e)["WeeklyOptHours"]
    
    # Aggregate weekly worked hours
    weekly_worked_hours = [timeslots_vars[(e, d, h)] for d in range(num_days) for h in range(hours_blocks)]
    
    # Weekly constraints
    model.Add(sum(weekly_worked_hours) >= weekly_min)
    model.Add(sum(weekly_worked_hours) <= weekly_max)
    
    for d in range(num_days):
        # Retrieve daily-specific constraints from the DataFrame
        daily_min = employees_df.row(e)["DailyMinHours"]
        daily_max = employees_df.row(e)["DailyMaxHours"]
        daily_opt = employees_df.row(e)["DailyOptHours"]
        
        # Aggregate daily worked hours
        daily_worked_hours = [timeslots_vars[(e, d, h)] for h in range(hours_blocks)]
        
        # Daily constraints
        model.Add(sum(daily_worked_hours) >= daily_min)
        model.Add(sum(daily_worked_hours) <= daily_max)
        


# Add constraint: At least two consecutive days off
for e in range(num_employees):
    # List to hold auxiliary variables indicating two consecutive days off
    consecutive_days_off = []
    for d in range(num_days - 1):
        # Create an auxiliary variable for days d and d+1 being off
        both_off = model.NewBoolVar(f"both_off_e{e}_d{d}_d{d+1}")
        consecutive_days_off.append(both_off)
        
        # Define the relationship:
        # both_off == 1 if and only if work_e_d[e][d] == 0 AND work_e_d[e][d+1] == 0
        model.Add(work_e_d[e][d] == 0).OnlyEnforceIf(both_off)
        model.Add(work_e_d[e][d+1] == 0).OnlyEnforceIf(both_off)
    
    # Ensure that at least one pair of consecutive days off exists
    model.Add(sum(consecutive_days_off) >= 1)


# Ensure that the workload by skill for each timeslot is respected.
# This constraint supposes that an employee with multiple skills can fill only one timeslot at the same time. Might not be true for all cases.
for ts in range(num_timeslots):
    for skill in skills:
        # Retrieve workload requirements for the current timeslot and skill
        workload = workload_df.filter(
            (pl.col('TimeslotID') == ts) & 
            (pl.col('SkillID') == skill)
        )
        if workload.height == 0:
            logging.debug(
                f"No workload defined for TimeslotID {ts} and SkillID {skill}. Skipping constraint."
            )
            continue
        min_workload = workload.row(0)['MinAmount']
        opt_workload = workload.row(0)['OptAmount']
        
        # Identify employees who have the required skill
        eligible_employees = employees_skills_df.filter(
            pl.col('SkillID') == skill
        ).select('EmployeeID').to_series().to_list()
        
        if not eligible_employees:
            logging.warning(
                f"No employees found with SkillID {skill} for TimeslotID {ts}."
            )
            model.Add(0 >= min_workload)  # This will make the model infeasible if min_workload > 0
            continue
        
        # Get day and hour directly from timeslots dataframe
        timeslot_row = timeslots_df.filter(pl.col('TimeslotID') == ts)
        day = timeslot_row.select('Day').row(0)[0]
        hour = timeslot_row.select('Hour').row(0)[0]
        
        assigned_vars = [timeslots_vars[(e, day, hour)] for e in eligible_employees]
        
        # Add constraints to meet the minimum workload and not exceed the optimal workload
        model.Add(sum(assigned_vars) >= min_workload)
        model.Add(sum(assigned_vars) <= opt_workload)
    
    # Constraint: An employee should not be booked multiple times in the same timeslot
    for e in range(num_employees):
        # Get day and hour directly from timeslots dataframe
        timeslot_row = timeslots_df.filter(pl.col('TimeslotID') == ts)
        day = timeslot_row.select('Day').row(0)[0] 
        hour = timeslot_row.select('Hour').row(0)[0]
        
        # Sum assignments across all skills for the employee in this timeslot
        employee_assignments = [
            timeslots_vars[(e, day, hour)] for skill in skills
        ]
        
        # Ensure the employee is assigned to at most one skill in this timeslot
        model.Add(sum(employee_assignments) <= 1)
