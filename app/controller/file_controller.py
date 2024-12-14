from fastapi import APIRouter, UploadFile, File, HTTPException, Depends
from app.domain.services.file_service import FileService

router = APIRouter()

# Endpoint para cargar el archivo
@router.post("/uploadj/", tags=["Version 1"])
async def upload_file(file: UploadFile = File(...), file_service: FileService = Depends()):
    try:
        file_location = await file_service.process_file(file)
        return {"message": "File processed successfully", "file_location": file_location}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Nuevo endpoint para listar todo el contenido del archivo CSV procesado
@router.get("/list_allj/", tags=["Version 1"])
async def list_all(file_service: FileService = Depends()):
    try:
        result = await file_service.list_all()
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Endpoint para filtrar por programa y promedio
@router.get("/filter_by_program_and_avgj/", tags=["Version 1"])
async def filter_by_program_and_avg(program: str = None, avg_threshold: float = 0.0, file_service: FileService = Depends()):
    try:
        result = await file_service.filter_by_program_and_avg(program, avg_threshold)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Endpoint para obtener el promedio general por género, ya sea para un programa específico o para todos los programas
@router.get("/average_by_genderj/", tags=["Version 1"])
async def average_by_gender(program: str = None, file_service: FileService = Depends()):
    try:
        result = await file_service.average_by_gender(program)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Endpoint para obtener el promedio general por programa sin distinguir el género
@router.get("/average_by_programj/", tags=["Version 1"])
async def average_by_program(file_service: FileService = Depends()):
    try:
        result = await file_service.average_by_program()
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Endpoint para obtener el porcentaje de hombres y mujeres por carrera y en general
@router.get("/gender_percentagej/", tags=["Version 1"])
async def gender_percentage(program: str = None, file_service: FileService = Depends()):
    try:
        result = await file_service.gender_percentage(program)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Endpoint para obtener el promedio de edad de hombres y mujeres en general, por carrera y sin distinguir por género
@router.get("/average_agej/", tags=["Version 1"])
async def average_age(program: str = None, by_gender: bool = True, file_service: FileService = Depends()):
    try:
        result = await file_service.average_age(program, by_gender)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Endpoint para Filtrar por Documento de Identidad
@router.get("/filter_by_document/", tags=["Version 1"])
async def filter_by_document(document_id: int, file_service: FileService = Depends()):
    try:
        result = await file_service.filter_by_document(document_id)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# Endpoint para Obtener los Datos de Situaciones Especiales
@router.get("/socioeconomic_filters/", tags=["Version 1"])
async def socioeconomic_filters(file_service: FileService = Depends()):
    try:
        result = await file_service.socioeconomic_filters()
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
