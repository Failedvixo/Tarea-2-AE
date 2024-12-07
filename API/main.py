from fastapi import FastAPI, HTTPException, Depends, status
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

# Función para convertir resultado de consulta a diccionario
def row_to_dict(cursor, row):
    keys = [column[0] for column in cursor.description]
    return dict(zip(keys, row))

# Validación de credenciales de admin
async def validate_credentials(username: str, password: str):
    admin = con.execute(
        "SELECT * FROM Admin WHERE username = ? AND password = ?",
        (username, password)
    ).fetchone()
    if not admin:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return True

# Validación de API Key de sensor
async def validate_sensor_api_key(sensor_api_key: str):
    result = con.execute("SELECT * FROM Sensor WHERE sensor_api_key = ?", (sensor_api_key,)).fetchone()
    if not result:
        raise HTTPException(status_code=400, detail="Invalid sensor API key")
    return row_to_dict(con, result)

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
            # Convertir el diccionario en un JSON válido
            json_record = json.dumps(record)
            timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')  # Formato compatible con TIMESTAMP
            con.execute(
                "INSERT INTO SensorData (data_id, sensor_id, timestamp, json_data) VALUES (NEXTVAL('data_id_seq'), ?, ?, ?)",
                (sensor_id, timestamp, json_record)
            )
        return {"message": "Sensor data inserted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Consulta de Sensor Data
@app.get("/api/v1/sensor_data")
async def get_sensor_data(
    from_time: int, 
    to_time: int, 
    sensor_ids: List[int], 
    company_api_key: str
):
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
              AND sd.timestamp BETWEEN ? AND ?
            """,
            (company_id, tuple(sensor_ids), from_time, to_time)
        ).fetchall()
        return [row_to_dict(con, row) for row in data]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Ejecución de la aplicación
if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
