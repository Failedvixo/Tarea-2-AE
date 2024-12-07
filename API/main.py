from fastapi import FastAPI, Header, HTTPException, Depends
from pydantic import BaseModel
import duckdb
import uuid
from typing import List

app = FastAPI()

# Conectar a la base de datos DuckDB
con = duckdb.connect('../DB/iot_platform.duckdb')

# Modelos
class Company(BaseModel):
    company_name: str

class Location(BaseModel):
    company_id: int
    location_name: str
    location_country: str
    location_city: str
    location_meta: str

class Sensor(BaseModel):
    location_id: int
    sensor_name: str
    sensor_category: str
    sensor_meta: str

class SensorDataInsert(BaseModel):
    json_data: List[dict]

# Validación de credenciales de admin
async def validate_credentials(username: str = Header(...), password: str = Header(...)):
    if username != "admin" or password != "secret":
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return True

# Validación de API Key de compañía
async def validate_company_api_key(company_api_key: str = Header(...)):
    company = con.execute("SELECT * FROM Company WHERE company_api_key = ?", (company_api_key,)).fetchone()
    if not company:
        raise HTTPException(status_code=401, detail="Invalid company API key")
    return company

# Validación de API Key de sensor
async def validate_sensor_api_key(sensor_api_key: str = Header(...)):
    sensor = con.execute("SELECT * FROM Sensor WHERE sensor_api_key = ?", (sensor_api_key,)).fetchone()
    if not sensor:
        raise HTTPException(status_code=401, detail="Invalid sensor API key")
    return sensor

# Endpoints de Admin
@app.post("/api/v1/admin/companies", dependencies=[Depends(validate_credentials)]) #funcional
async def create_company(data: Company):
    try:
        company_api_key = str(uuid.uuid4())
        con.execute(
            "INSERT INTO Company (company_name, company_api_key) VALUES (?, ?)",
            (data.company_name, company_api_key)
        )
        return {
            "message": "Company created successfully",
            "company_name": data.company_name,
            "company_api_key": company_api_key
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/api/v1/admin/locations", dependencies=[Depends(validate_credentials)]) #funcional
async def create_location(location: Location):
    try:
        con.execute(
            """
            INSERT INTO Location (location_id, company_id, location_name, location_country, location_city, location_meta) 
            VALUES (NEXTVAL('location_id_seq'), ?, ?, ?, ?, ?)
            """,
            (location.company_id, location.location_name, location.location_country, location.location_city, location.location_meta)
        )
        return {"message": "Location created successfully", "location": location}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/api/v1/admin/sensors", dependencies=[Depends(validate_credentials)])
async def create_sensor(sensor: Sensor):
    try:
        sensor_api_key = str(uuid.uuid4())
        con.execute(
            "INSERT INTO Sensor (location_id, sensor_name, sensor_category, sensor_meta, sensor_api_key) VALUES (?, ?, ?, ?, ?)",
            (sensor.location_id, sensor.sensor_name, sensor.sensor_category, sensor.sensor_meta, sensor_api_key)
        )
        return {
            "message": "Sensor created successfully",
            "sensor_name": sensor.sensor_name,
            "sensor_api_key": sensor_api_key
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# CRUD para Location
@app.get("/api/v1/locations", dependencies=[Depends(validate_company_api_key)])
async def get_locations():
    return con.execute("SELECT * FROM Location").fetchall()

@app.get("/api/v1/locations/{location_id}", dependencies=[Depends(validate_company_api_key)])
async def get_location(location_id: int):
    location = con.execute("SELECT * FROM Location WHERE location_id = ?", (location_id,)).fetchone()
    if not location:
        raise HTTPException(status_code=404, detail="Location not found")
    return location

@app.put("/api/v1/locations/{location_id}", dependencies=[Depends(validate_company_api_key)])
async def update_location(location_id: int, location: Location):
    con.execute(
        "UPDATE Location SET location_name = ?, location_country = ?, location_city = ?, location_meta = ? WHERE location_id = ?",
        (location.location_name, location.location_country, location.location_city, location.location_meta, location_id)
    )
    return {"message": "Location updated successfully"}

@app.delete("/api/v1/locations/{location_id}", dependencies=[Depends(validate_company_api_key)])
async def delete_location(location_id: int):
    con.execute("DELETE FROM Location WHERE location_id = ?", (location_id,))
    return {"message": "Location deleted successfully"}

# CRUD para Sensor y Sensor Data sería similar

# Inserción de Sensor Data
@app.post("/api/v1/sensor_data")
async def insert_sensor_data(data: SensorDataInsert, sensor_api_key: str = Header(...)):
    sensor = await validate_sensor_api_key(sensor_api_key)
    try:
        for record in data.json_data:
            con.execute(
                "INSERT INTO SensorData (sensor_id, data) VALUES (?, ?)",
                (sensor["sensor_id"], record)
            )
        return {"message": "Sensor data inserted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Consulta de Sensor Data
@app.get("/api/v1/sensor_data")
async def get_sensor_data(from_time: int, to_time: int, sensor_ids: List[int], company_api_key: str = Header(...)):
    await validate_company_api_key(company_api_key)
    try:
        data = con.execute(
            "SELECT * FROM SensorData WHERE sensor_id IN ? AND timestamp BETWEEN ? AND ?",
            (tuple(sensor_ids), from_time, to_time)
        ).fetchall()
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Ejecución de la aplicación
if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
