import pandas as pd

class ExcelRepository:
    def __init__(self, file_path):
        self.file_path = file_path
    
    def read_excel_file(self):
        try:
            return pd.read_excel(self.file_path)
        except Exception as e:
            raise FileNotFoundError(f"Error al leer el archivo: {str(e)}")
    
    def save_excel_file(self, df, output_file):
        df.to_excel(output_file, index=False)
