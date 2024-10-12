from app.persistance.repository.excel_repository import ExcelRepository 
import pandas as pd

class ExcelService:
    def __init__(self, excel_repo: ExcelRepository):
        self.excel_repo = excel_repo

    def process_file(self):
        df = self.excel_repo.read_excel_file()
        selected_columns = [
            "UNIDAD",  "PERIODO", "PEGE_DOCUMENTOIDENTIDAD", 
            # más columnas aquí
        ]
        
        # Verificar si las columnas existen en el archivo
        missing_columns = [col for col in selected_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Columnas faltantes: {missing_columns}")
        
        # Filtrar las columnas seleccionadas
        selected_df = df[selected_columns]
        return selected_df
    
    def filter_by_program_and_avg(self, df, program=None, avg_threshold=0.0):
        if program:
            df = df[df['PROGRAMA'] == program]
        return df[df['ESTP_PROMEDIOGENERAL'] > avg_threshold]
