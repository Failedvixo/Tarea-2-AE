from fastapi import FastAPI
from admin_routes import admin_router
from user_routes import user_router

app = FastAPI()

# Registrar las rutas
app.include_router(admin_router, prefix="/api/v1/admin", tags=["Admin"])
app.include_router(user_router, prefix="/api/v1", tags=["User"])
