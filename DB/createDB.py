import duckdb

# Ruta de la base de datos
db_path = 'iot_platform.duckdb'

# Conexión a DuckDB
conn = duckdb.connect(db_path)

# Crear la secuencia para Company
conn.execute("""
CREATE SEQUENCE IF NOT EXISTS company_id_seq START 1 INCREMENT 1;
""")

# Crear la secuencia para Location
conn.execute("""
CREATE SEQUENCE IF NOT EXISTS location_id_seq START 1 INCREMENT 1;
""")

# Crear las tablas
conn.execute("""
-- Tabla Admin
CREATE TABLE IF NOT EXISTS Admin (
    username VARCHAR(255) NOT NULL,
    password VARCHAR(255) NOT NULL
);
""")

conn.execute("""
-- Tabla Company
CREATE TABLE IF NOT EXISTS Company (
    company_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('company_id_seq'),
    company_name VARCHAR(255) NOT NULL,
    company_api_key VARCHAR(255) NOT NULL
);
""")

conn.execute("""
-- Tabla Location
CREATE TABLE IF NOT EXISTS Location (
    location_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('location_id_seq'),
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
CREATE TABLE IF NOT EXISTS Sensor (
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
CREATE TABLE IF NOT EXISTS SensorData (
    data_id INTEGER PRIMARY KEY,
    sensor_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    json_data JSON NOT NULL,
    FOREIGN KEY (sensor_id) REFERENCES Sensor(sensor_id)
);
""")

# Insertar un administrador predeterminado
admin_exists = conn.execute("SELECT COUNT(*) FROM Admin WHERE username = 'admin'").fetchone()[0]
if admin_exists == 0:
    conn.execute("""
    INSERT INTO Admin (username, password) 
    VALUES ('admin', 'admin');
    """)
    print("Usuario administrador creado: username=admin, password=admin")
else:
    print("Usuario administrador ya existe.")

# Ejemplo de inserción de datos en las tablas


print("Base de datos creada correctamente en:", db_path)

# Cerrar conexión
conn.close()
