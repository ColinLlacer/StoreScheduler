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

