from fastapi import FastAPI, HTTPException, Depends, status, Query
from pydantic import BaseModel
import duckdb
import uuid
from typing import List
from datetime import datetime
import json

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
    api_key: str
    json_data: List[dict]

# Validación de API Key de sensor
async def validate_sensor_api_key(sensor_api_key: str):
    result = con.execute("SELECT * FROM Sensor WHERE sensor_api_key = ?", (sensor_api_key,)).fetchone()
    if not result:
        raise HTTPException(status_code=400, detail="Invalid sensor API key")
    return result

# Endpoints de Admin
@app.post("/api/v1/admin/companies")
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

@app.post("/api/v1/admin/locations")
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

@app.post("/api/v1/admin/sensors")
async def create_sensor(sensor: Sensor):
    try:
        sensor_api_key = str(uuid.uuid4())
        con.execute(
            "INSERT INTO Sensor (sensor_id, location_id, sensor_name, sensor_category, sensor_meta, sensor_api_key) VALUES (NEXTVAL('sensor_id_seq'), ?, ?, ?, ?, ?)",
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
@app.get("/api/v1/locations")
async def get_locations(company=Depends(validate_company_api_key)):
    locations = con.execute(
        "SELECT * FROM Location WHERE company_id = ?", 
        (company["company_id"],)
    ).fetchall()
    return [row_to_dict(con, row) for row in locations]

@app.get("/api/v1/locations/{location_id}")
async def get_location(location_id: int, company=Depends(validate_company_api_key)):
    location = con.execute(
        "SELECT * FROM Location WHERE location_id = ? AND company_id = ?", 
        (location_id, company["company_id"])
    ).fetchone()
    if not location:
        raise HTTPException(status_code=404, detail="Location not found")
    return row_to_dict(con, location)

@app.put("/api/v1/locations/{location_id}")
async def update_location(location_id: int, location: Location, company=Depends(validate_company_api_key)):
    affected_rows = con.execute(
        "UPDATE Location SET location_name = ?, location_country = ?, location_city = ?, location_meta = ? WHERE location_id = ? AND company_id = ?",
        (location.location_name, location.location_country, location.location_city, location.location_meta, location_id, company["company_id"])
    ).rowcount
    if affected_rows == 0:
        raise HTTPException(status_code=404, detail="Location not found or does not belong to your company")
    return {"message": "Location updated successfully"}

@app.delete("/api/v1/locations/{location_id}")
async def delete_location(location_id: int, company=Depends(validate_company_api_key)):
    affected_rows = con.execute(
        "DELETE FROM Location WHERE location_id = ? AND company_id = ?", 
        (location_id, company["company_id"])
    ).rowcount
    if affected_rows == 0:
        raise HTTPException(status_code=404, detail="Location not found or does not belong to your company")
    return {"message": "Location deleted successfully"}

# CRUD para Sensor
@app.get("/api/v1/sensors")
async def get_sensors(company=Depends(validate_company_api_key)):
    sensors = con.execute(
        """
        SELECT * FROM Sensor
        WHERE location_id IN (
            SELECT location_id 
            FROM Location 
            WHERE company_id = ?
        )
        """,
        (company["company_id"],)
    ).fetchall()
    return [row_to_dict(con, row) for row in sensors]

@app.get("/api/v1/sensors/{sensor_id}")
async def get_sensor(sensor_id: int, company=Depends(validate_company_api_key)):
    sensor = con.execute(
        """
        SELECT * FROM Sensor
        WHERE sensor_id = ? AND location_id IN (
            SELECT location_id 
            FROM Location 
            WHERE company_id = ?
        )
        """,
        (sensor_id, company["company_id"])
    ).fetchone()
    if not sensor:
        raise HTTPException(status_code=404, detail="Sensor not found or does not belong to your company")
    return row_to_dict(con, sensor)

@app.put("/api/v1/sensors/{sensor_id}")
async def update_sensor(sensor_id: int, sensor: Sensor, company=Depends(validate_company_api_key)):
    affected_rows = con.execute(
        """
        UPDATE Sensor
        SET sensor_name = ?, sensor_category = ?, sensor_meta = ?
        WHERE sensor_id = ? AND location_id IN (
            SELECT location_id 
            FROM Location 
            WHERE company_id = ?
        )
        """,
        (sensor.sensor_name, sensor.sensor_category, sensor.sensor_meta, sensor_id, company["company_id"])
    ).rowcount
    if affected_rows == 0:
        raise HTTPException(status_code=404, detail="Sensor not found or does not belong to your company")
    return {"message": "Sensor updated successfully"}

@app.delete("/api/v1/sensors/{sensor_id}")
async def delete_sensor(sensor_id: int, company=Depends(validate_company_api_key)):
    affected_rows = con.execute(
        """
        DELETE FROM Sensor
        WHERE sensor_id = ? AND location_id IN (
            SELECT location_id 
            FROM Location 
            WHERE company_id = ?
        )
        """,
        (sensor_id, company["company_id"])
    ).rowcount
    if affected_rows == 0:
        raise HTTPException(status_code=404, detail="Sensor not found or does not belong to your company")
    return {"message": "Sensor deleted successfully"}

# Inserción de Sensor Data
@app.post("/api/v1/sensor_data", status_code=status.HTTP_201_CREATED)
async def insert_sensor_data(data: SensorDataInsert):
    sensor = con.execute(
        "SELECT sensor_id FROM Sensor WHERE sensor_api_key = ?", 
        (data.api_key,)
    ).fetchone()
    if not sensor:
        raise HTTPException(status_code=400, detail="Invalid sensor API key")
    sensor_id = sensor[0]

    try:
        for record in data.json_data:
            json_record = json.dumps(record)
            timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            con.execute(
                "INSERT INTO SensorData (data_id, sensor_id, timestamp, json_data) VALUES (NEXTVAL('data_id_seq'), ?, ?, ?)",
                (sensor_id, timestamp, json_record)
            )
        return {"message": "Sensor data inserted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Consulta de Sensor Data
@app.get("/api/v1/sensor_data", status_code=status.HTTP_200_OK)
async def get_sensor_data(
    company_api_key: str,
    from_time: int,
    to_time: int,
    sensor_ids: str = Query(...)
):
    try:
        # Intentar cargar `sensor_ids` como JSON
        sensor_ids_list = json.loads(sensor_ids)

        # Validar que sea una lista de enteros
        if not isinstance(sensor_ids_list, list) or not all(isinstance(i, int) for i in sensor_ids_list):
            raise ValueError("sensor_ids must be a list of integers")
    except (json.JSONDecodeError, ValueError):
        raise HTTPException(status_code=422, detail="Invalid format for sensor_ids. Must be a JSON-like list of integers.")
    
    # Validar el API Key de la compañía
    company = con.execute(
        "SELECT company_id FROM Company WHERE company_api_key = ?", 
        (company_api_key,)
    ).fetchone()
    if not company:
        raise HTTPException(status_code=401, detail="Invalid company API key")
    company_id = company[0]

    try:
        data = con.execute(
            """
            SELECT sd.sensor_id, sd.json_data AS data, sd.timestamp
            FROM SensorData sd
            JOIN Sensor s ON sd.sensor_id = s.sensor_id
            JOIN Location l ON s.location_id = l.location_id
            WHERE l.company_id = ? 
              AND sd.sensor_id IN ? 
              AND sd.timestamp BETWEEN TO_TIMESTAMP(?) AND TO_TIMESTAMP(?)
            """,
            (company_id, tuple(sensor_ids_list), from_time, to_time)
        ).fetchall()

        results = [
            {"sensor_id": row[0], "data": json.loads(row[1]), "timestamp": row[2].isoformat()} 
            for row in data
        ]

        return {"sensor_data": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Ejecución de la aplicación
if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
