import duckdb

con = duckdb.connect('iot_platform.duckdb')

companies = con.execute("SELECT * FROM Sensor").fetchall()

for company in companies:
    print(company)
