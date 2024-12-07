import duckdb

con = duckdb.connect('iot_platform.duckdb')

companies = con.execute("SELECT company_id, company_name, company_api_key FROM Company").fetchall()

for company in companies:
    print(f"Company ID: {company[0]}, Name: {company[1]}, API Key: {company[2]}")
