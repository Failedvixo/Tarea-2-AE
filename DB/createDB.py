import duckdb

# Ruta de la base de datos
db_path = 'iot_platform.duckdb'

# Conexión a DuckDB
conn = duckdb.connect(db_path)

# Crear las tablas
conn.execute("""
-- Tabla Admin
CREATE TABLE Admin (
    username VARCHAR(255) NOT NULL,
    password VARCHAR(255) NOT NULL
);
""")

conn.execute("""
-- Tabla Company
CREATE TABLE Company (
    company_id INTEGER PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    company_api_key VARCHAR(255) NOT NULL
);
""")

conn.execute("""
-- Tabla Location
CREATE TABLE Location (
    location_id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL,
    location_name VARCHAR(255) NOT NULL,
    location_country VARCHAR(255),
    location_city VARCHAR(255),
    location_meta TEXT,
    FOREIGN KEY (company_id) REFERENCES Company(company_id)
);
""")

conn.execute("""
-- Tabla Sensor
CREATE TABLE Sensor (
    sensor_id INTEGER PRIMARY KEY,
    location_id INTEGER NOT NULL,
    sensor_name VARCHAR(255) NOT NULL,
    sensor_category VARCHAR(255),
    sensor_meta TEXT,
    sensor_api_key VARCHAR(255) NOT NULL,
    FOREIGN KEY (location_id) REFERENCES Location(location_id)
);
""")

conn.execute("""
-- Tabla SensorData
CREATE TABLE SensorData (
    data_id INTEGER PRIMARY KEY,
    sensor_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    json_data JSON NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES Sensor(sensor_id)
);
""")

# Mensaje de confirmación
print(f"Base de datos creada correctamente en: {db_path}")

# Cerrar conexión
conn.close()
