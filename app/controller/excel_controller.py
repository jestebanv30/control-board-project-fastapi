from fastapi import APIRouter, HTTPException, UploadFile, File
from app.domain.services.excel_service import ExcelService
from app.persistance.repository.excel_repository import ExcelRepository

router = APIRouter()

@router.post("/uploadfile/")
async def upload_file(file: UploadFile = File(...)):
    file_location = save_uploaded_file(file)  # función para guardar el archivo
    excel_repo = ExcelRepository(file_location)
    excel_service = ExcelService(excel_repo)
    
    try:
        df = excel_service.process_file()
        excel_repo.save_excel_file(df, "files/processed_data.xlsx")
        return {"detail": "Archivo procesado exitosamente"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
