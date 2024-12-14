import os
import pandas as pd
from fastapi import UploadFile

class FileRepository:
    FILE_PATH = r"C:\Users\valde\Documents\fastAPI-projects\Proyecto Tablero de Control\control-board-project-fastapi\data\processed_data.json"

    async def save_temp_file(self, file: UploadFile):
        # Guardar el archivo en la ruta especificada
        suffix = os.path.splitext(file.filename)[1]
        file_path = self.FILE_PATH.replace(".csv", suffix)  # Reemplazar la extensión si no es CSV
        with open(file_path, "wb") as temp_file:
            temp_file.write(await file.read())
        return file_path

    async def load_data(self):
        try:
            # Cargar el archivo desde la nueva ruta
            df = pd.read_json(self.FILE_PATH)
            return df
        except FileNotFoundError:
            raise Exception("Processed file not found.")
