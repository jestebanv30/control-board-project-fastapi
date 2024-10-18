from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.controller import file_controller

app = FastAPI()

# Configuración de CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(file_controller.router)

@app.get("/prueba")
async def ping():
    return {"message": "prueba OK"}