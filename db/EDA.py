import duckdb
import os

current_directory = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(current_directory, '..', 'db', 'db.duckdb')
print(db_path)

con = duckdb.connect(db_path)

con.sql("SELECT * FROM Employees").show()
con.sql("SELECT * FROM Skills").show()
con.sql("SELECT * FROM Codes").show()
con.sql("SELECT * FROM Roles").show()
con.sql("SELECT * FROM Status").show()
con.sql("SELECT * FROM Store").show()
con.sql("SELECT * FROM Timeslot").show()
con.sql("SELECT * FROM EmployeesSkills").show()
con.sql("SELECT * FROM Availability_Preferences").show()
con.sql("SELECT * FROM Workload").show()

con.close()