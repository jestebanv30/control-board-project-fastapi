from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from app.domain.services.file_service import FileService

router = APIRouter()

# Endpoint para cargar el archivo
@router.post("/upload/")
async def upload_file(file: UploadFile = File(...), file_service: FileService = Depends()):
    try:
        file_location = await file_service.process_file(file)
        return {"message": "File processed successfully", "file_location": file_location}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Nuevo endpoint para listar todo el contenido del archivo CSV procesado
@router.get("/list_all/")
async def list_all(file_service: FileService = Depends()):
    try:
        result = await file_service.list_all()
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Endpoint para cargar el archivo
#@router.post("/upload/")
#async def upload_file(file: UploadFile = File(...), file_service: FileService = Depends()):
#    try:
#        # Llamar al servicio para manejar la lógica de guardar y convertir a CSV si es necesario
#        file_location = await file_service.process_file(file)
#        return {"message": "File processed successfully", "file_location": file_location}
#    except Exception as e:
#        raise HTTPException(status_code=400, detail=str(e))

# Endpoint para filtrar por programa y promedio
@router.get("/filter_by_program_and_avg/")
async def filter_by_program_and_avg(program: str = None, avg_threshold: float = 0.0, file_service: FileService = Depends()):
    try:
        result = await file_service.filter_by_program_and_avg(program, avg_threshold)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Endpoint para obtener el promedio general por género, ya sea para un programa específico o para todos los programas
@router.get("/average_by_gender/")
async def average_by_gender(program: str = None, file_service: FileService = Depends()):
    try:
        result = await file_service.average_by_gender(program)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Endpoint para obtener el promedio general por programa sin distinguir el género
@router.get("/average_by_program/")
async def average_by_program(file_service: FileService = Depends()):
    try:
        result = await file_service.average_by_program()
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Endpoint para obtener el porcentaje de hombres y mujeres por carrera y en general
@router.get("/gender_percentage/")
async def gender_percentage(program: str = None, file_service: FileService = Depends()):
    try:
        result = await file_service.gender_percentage(program)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Endpoint para obtener el promedio de edad de hombres y mujeres en general, por carrera y sin distinguir por género
@router.get("/average_age/")
async def average_age(program: str = None, by_gender: bool = True, file_service: FileService = Depends()):
    try:
        result = await file_service.average_age(program, by_gender)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
