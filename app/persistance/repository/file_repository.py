import os
import pandas as pd
from fastapi import UploadFile

class FileRepository:
    FILE_PATH = r"C:\Users\valde\Escritorio\files\processed_data.csv"

    async def save_temp_file(self, file: UploadFile):
        # Guardar el archivo en la ruta especificada
        suffix = os.path.splitext(file.filename)[1]
        file_path = self.FILE_PATH.replace(".csv", suffix)  # Reemplazar la extensión si no es CSV
        with open(file_path, "wb") as temp_file:
            temp_file.write(await file.read())
        return file_path

    async def load_csv(self):
        try:
            # Cargar el archivo desde la nueva ruta
            return pd.read_csv(self.FILE_PATH)
        except FileNotFoundError:
            raise Exception("Processed file not found.")
