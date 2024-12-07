from fastapi import APIRouter, HTTPException, Header
import duckdb
from uuid import uuid4

admin_router = APIRouter()

# Datos del administrador
ADMIN_CREDENTIALS = {"username": "admin", "password": "admin123"}

def authenticate_admin(username: str, password: str):
    if username == ADMIN_CREDENTIALS["username"] and password == ADMIN_CREDENTIALS["password"]:
        return True
    raise HTTPException(status_code=401, detail="Invalid credentials")

con = duckdb.connect('iot_platform.duckdb')

@admin_router.post('/companies')
async def create_company(company_name: str, username: str = Header(...), password: str = Header(...)):
    # Validar administrador
    if not authenticate_admin(username, password):
        raise HTTPException(status_code=403, detail="Unauthorized")

    # Generar API Key
    company_api_key = str(uuid4())

    # Insertar en la base de datos
    con.execute(
        "INSERT INTO Company (company_name, company_api_key) VALUES (?, ?)",
        [company_name, company_api_key]
    )

    return {"message": "Company created successfully", "company_name": company_name, "company_api_key": company_api_key}

@admin_router.get('/companies')
async def list_companies(username: str = Header(...), password: str = Header(...)):
    # Validar administrador
    if not authenticate_admin(username, password):
        raise HTTPException(status_code=403, detail="Unauthorized")

    companies = con.execute("SELECT company_id, company_name, company_api_key FROM Company").fetchall()
    return [
        {"company_id": company[0], "company_name": company[1], "company_api_key": company[2]}
        for company in companies
    ]
