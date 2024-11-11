from ortools.sat.python import cp_model
import duckdb

# Connect to the database
con = duckdb.connect("db.duckdb")
model = cp_model.CpModel()

# Fetch data
employees = con.execute("SELECT COUNT(*) FROM Employees").fetchone()[0]
timeslots = con.execute("SELECT * FROM Timeslot").fetchall()
hours_blocks = 24

num_employees = employees
num_days = con.execute("SELECT COUNT(DISTINCT DATE(Datetime)) as days FROM Timeslot").fetchone()[0]

# Create Boolean variables for each timeblock
timeslots_vars = {}
for e in range(num_employees):
    for d in range(num_days):
        for h in range(hours_blocks):
            timeslots_vars[(e, d, h)] = model.NewBoolVar(f"shift_{e}_{d}_{h}")

# Add transition constraints to ensure that timeslots assigned to an employee are consecutive
for e in range(num_employees):
    for d in range(num_days):
        transition_vars = []
        for h in range(hours_blocks - 1):
            # Create a transition variable: 1 if there's a change between h and h+1
            transition = model.NewBoolVar(f"transition_e{e}_d{d}_h{h}")
            # If current hour is different from the next hour, transition is 1
            model.Add(timeslots_vars[(e, d, h)] != timeslots_vars[(e, d, h + 1)]).OnlyEnforceIf(transition)
            model.Add(timeslots_vars[(e, d, h)] == timeslots_vars[(e, d, h + 1)]).OnlyEnforceIf(transition.Not())
            transition_vars.append(transition)
        
        # The sum of transitions should be <= 2 (one start and one end)
        model.Add(sum(transition_vars) <= 2)

# Check minimum and maximum hours constraints (daily and weekly)
for e in range(num_employees):
    # Aggregate weekly worked hours using list comprehension
    weekly_worked_hours = [timeslots_vars[(e, d, h)] for d in range(num_days) for h in range(hours_blocks)]
    
    # Weekly constraints
    model.Add(sum(weekly_worked_hours) >= employees[e].WeeklyMinHours)
    model.Add(sum(weekly_worked_hours) <= employees[e].WeeklyMaxHours)
    
    # Weekly optimal hours soft constraint
    weekly_delta = model.NewIntVar(-168, 168, f'weekly_delta_{e}')
    model.Add(weekly_delta == sum(weekly_worked_hours) - employees[e].WeeklyOptHours)
    
    for d in range(num_days):
        # Aggregate daily worked hours using list comprehension
        daily_worked_hours = [timeslots_vars[(e, d, h)] for h in range(hours_blocks)]
        
        # Daily constraints
        model.Add(sum(daily_worked_hours) >= employees[e].DailyMinHours)
        model.Add(sum(daily_worked_hours) <= employees[e].DailyMaxHours)
        
        # Daily optimal hours soft constraint
        daily_delta = model.NewIntVar(-24, 24, f'daily_delta_{e}_{d}')
        model.Add(daily_delta == sum(daily_worked_hours) - employees[e].DailyOptHours)





