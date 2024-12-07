from fastapi import APIRouter, HTTPException, Query, Header
from pydantic import BaseModel
import duckdb
from typing import List, Optional

# Crear el router para usuarios
user_router = APIRouter()

# Conexión a la base de datos
con = duckdb.connect('iot_platform.duckdb')

# Modelos Pydantic
class Location(BaseModel):
    location_name: str
    location_country: Optional[str]
    location_city: Optional[str]
    location_meta: Optional[str]

class Sensor(BaseModel):
    location_id: int
    sensor_name: str
    sensor_category: Optional[str]
    sensor_meta: Optional[str]
    sensor_api_key: str

class SensorData(BaseModel):
    sensor_api_key: str
    timestamp: int  # Epoch timestamp
    json_data: dict

# CRUD para Location
@user_router.post('/locations')
async def create_location(location: Location, company_api_key: str = Header(...)):
    company = con.execute("SELECT company_id FROM Company WHERE company_api_key = ?", [company_api_key]).fetchone()
    if not company:
        raise HTTPException(status_code=403, detail="Invalid company API key")
    
    con.execute(
        """
        INSERT INTO Location (company_id, location_name, location_country, location_city, location_meta) 
        VALUES (?, ?, ?, ?, ?)
        """,
        [company[0], location.location_name, location.location_country, location.location_city, location.location_meta]
    )
    return {"message": "Location created successfully", "status": 201}

@user_router.get('/locations')
async def get_locations(company_api_key: str = Header(...)):
    company = con.execute("SELECT company_id FROM Company WHERE company_api_key = ?", [company_api_key]).fetchone()
    if not company:
        raise HTTPException(status_code=403, detail="Invalid company API key")
    
    locations = con.execute("SELECT * FROM Location WHERE company_id = ?", [company[0]]).fetchall()
    return [
        {
            "location_id": loc[0],
            "company_id": loc[1],
            "location_name": loc[2],
            "location_country": loc[3],
            "location_city": loc[4],
            "location_meta": loc[5],
        }
        for loc in locations
    ]

# CRUD para Sensor
@user_router.post('/sensors')
async def create_sensor(sensor: Sensor, company_api_key: str = Header(...)):
    location = con.execute("SELECT location_id FROM Location WHERE location_id = ? AND company_id = (SELECT company_id FROM Company WHERE company_api_key = ?)", [sensor.location_id, company_api_key]).fetchone()
    if not location:
        raise HTTPException(status_code=403, detail="Invalid location or company")
    
    con.execute(
        """
        INSERT INTO Sensor (location_id, sensor_name, sensor_category, sensor_meta, sensor_api_key) 
        VALUES (?, ?, ?, ?, ?)
        """,
        [sensor.location_id, sensor.sensor_name, sensor.sensor_category, sensor.sensor_meta, sensor.sensor_api_key]
    )
    return {"message": "Sensor created successfully", "status": 201}

@user_router.get('/sensors')
async def get_sensors(location_id: int, company_api_key: str = Header(...)):
    company = con.execute("SELECT company_id FROM Company WHERE company_api_key = ?", [company_api_key]).fetchone()
    if not company:
        raise HTTPException(status_code=403, detail="Invalid company API key")
    
    sensors = con.execute("SELECT * FROM Sensor WHERE location_id = ?", [location_id]).fetchall()
    return [{"sensor_id": s[0], "location_id": s[1], "sensor_name": s[2], "sensor_category": s[3], "sensor_meta": s[4], "sensor_api_key": s[5]} for s in sensors]

# CRUD para SensorData
@user_router.post('/sensor_data')
async def insert_sensor_data(data: List[SensorData]):
    for entry in data:
        sensor = con.execute("SELECT sensor_id FROM Sensor WHERE sensor_api_key = ?", [entry.sensor_api_key]).fetchone()
        if not sensor:
            raise HTTPException(status_code=400, detail="Invalid sensor API key")

        con.execute(
            "INSERT INTO SensorData (sensor_id, timestamp, json_data) VALUES (?, ?, ?)",
            [sensor[0], entry.timestamp, entry.json_data]
        )
    return {"message": "Sensor data inserted successfully", "status": 201}

@user_router.get('/sensor_data')
async def get_sensor_data(
    company_api_key: str = Header(...),
    from_timestamp: int = Query(...),
    to_timestamp: int = Query(...),
    sensor_ids: List[int] = Query(...)
):
    company = con.execute("SELECT company_id FROM Company WHERE company_api_key = ?", [company_api_key]).fetchone()
    if not company:
        raise HTTPException(status_code=403, detail="Invalid company API key")
    
    query = f"""
        SELECT sd.sensor_id, sd.timestamp, sd.json_data 
        FROM SensorData sd
        JOIN Sensor s ON sd.sensor_id = s.sensor_id
        JOIN Location l ON s.location_id = l.location_id
        WHERE l.company_id = ? AND sd.sensor_id IN ({','.join(['?'] * len(sensor_ids))})
        AND sd.timestamp BETWEEN ? AND ?
    """
    params = [company[0], *sensor_ids, from_timestamp, to_timestamp]
    results = con.execute(query, params).fetchall()

    return [
        {"sensor_id": res[0], "timestamp": res[1], "json_data": res[2]}
        for res in results
    ]
