import os
import pandas as pd
from fastapi import UploadFile
from fastapi import HTTPException
from app.persistance.repository.file_repository import FileRepository

SELECTED_COLUMNS = [
    "UNIDAD",  "PERIODO", "PEGE_DOCUMENTOIDENTIDAD", "PENG_PRIMERAPELLIDO", "PENG_SEGUNDOAPELLIDO", 
    "PENG_PRIMERNOMBRE", "PENG_SEGUNDONOMBRE", "PROG_CODIGOPROGRAMA", "PROGRAMA", "FECHA_INGRESO_ESTP", 
    "FECHA_INGRESO_ESTU", "PENSUM_DESCRIPCION", "JORN_DESCRIPCION", "ESTADO_PENSUM", "PESUM_CREDITOS", 
    "PENG_EMAILINSTITUCIONAL", "INFE_ESTRATO", "ESTP_JOVENACCION", "ESTP_GENERACION_E", "ESTP_VICTIMA", 
    "ESTP_AFRO", "PENG_FECHANACIMIENTO", "EDAD", "PENG_SEXO", "MATERIAS_TOMADAS", "PAGE_IDNACIMIENTO", 
    "PAGE_IDNACIONALIDAD", "CIGE_IDLUGARNACIMIENTO", "ESTP_PERIODOACADEMICO", "ESTP_PERIODOCRONOLOGICO", 
    "ESTP_PROMEDIOGENERAL", "ESTP_PROMEDIOSEMESTRE", "ESTP_CREDITOSAPROBADOS", "SITE_DESCRIPCION"
]

class FileService:
    def __init__(self):
        self.file_repository = FileRepository()

    async def process_file(self, file: UploadFile):
        file_extension = os.path.splitext(file.filename)[1]
        
        if file_extension.lower() not in ['.csv', '.xlsx']:
            raise Exception("Invalid file type. Only CSV and Excel files are allowed.")

        # Si es un archivo Excel, convertirlo a CSV
        if file_extension.lower() == ".xlsx":
            temp_file_path = await self.file_repository.save_temp_file(file)
            df = pd.read_excel(temp_file_path)
            df.to_csv(self.file_repository.FILE_PATH, index=False) # Guardar el archivo CSV en la ruta especificada
        else:
            csv_file_path = await self.file_repository.save_temp_file(file)

        return csv_file_path

    async def validate_columns(self, df):
        missing_columns = [col for col in SELECTED_COLUMNS if col not in df.columns]
        if missing_columns:
            raise Exception(f"Missing columns: {missing_columns}")
        return df[SELECTED_COLUMNS]

    async def filter_by_program_and_avg(self, program: str, avg_threshold: float):
        try:
            # Cargar el archivo CSV
            df = await self.file_repository.load_csv()

            # Rellenar NaN en la columna "ESTP_PROMEDIOGENERAL" con 0
            df["ESTP_PROMEDIOGENERAL"] = df["ESTP_PROMEDIOGENERAL"].fillna(0)

            # Convertir a float explícitamente si es necesario
            df["ESTP_PROMEDIOGENERAL"] = df["ESTP_PROMEDIOGENERAL"].astype(float)

            # Reemplazar infinitos por 0
            df = df.replace([float('inf'), float('-inf')], 0)

            # Filtrar por programa y promedio
            filtered_df = df[(df['PROGRAMA'] == program) & (df['ESTP_PROMEDIOGENERAL'] >= avg_threshold)]

            # Retorna el DataFrame filtrado
            return filtered_df.to_dict(orient='records')

        except KeyError as e:
            raise HTTPException(status_code=400, detail=f"Column not found: {str(e)}")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Data type error: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
    ##async def filter_by_program_and_avg(self, program: str, avg_threshold: float):
         # Cargar el archivo CSV
        ##df = await self.file_repository.load_csv()

        # Reemplazar valores NaN o infinitos en la columna ESTP_PROMEDIOGENERAL
        ##df["ESTP_PROMEDIOGENERAL"] = pd.to_numeric(df["ESTP_PROMEDIOGENERAL"], errors='coerce')  # Convierte a numérico, NaN si no es posible

        # Opcional: llenar los NaN con un valor predeterminado o eliminar filas con NaN
        ##df["ESTP_PROMEDIOGENERAL"].fillna(0, inplace=True)

        # Filtrar por programa y por el puntaje mayor al umbral
        ##filtered_df = df[
        ##    (df["PROGRAMA"].str.contains(program, case=False, na=False)) & 
        ##    (df["ESTP_PROMEDIOGENERAL"] >= avg_threshold)
        ##]

         # Eliminar NaN o valores infinitos que puedan causar problemas en la respuesta JSON
        ##filtered_df.replace([float('inf'), float('-inf')], 0, inplace=True)  # Reemplazar infinitos con 0

        # Seleccionar las columnas requeridas
        ##filtered_df = filtered_df[SELECTED_COLUMNS]

        # Convertir a una lista de diccionarios para devolver como JSON
        ##return filtered_df.to_dict(orient="records")

        #df_filtered = df[df['PROGRAMA'] == program] if program else df
        #result = df_filtered[df_filtered['ESTP_PROMEDIOGENERAL'] > avg_threshold]
        #return result[['PENG_PRIMERNOMBRE', 'PENG_PRIMERAPELLIDO', 'PEGE_DOCUMENTOIDENTIDAD', 'PROGRAMA', 'ESTP_PROMEDIOGENERAL']].to_dict(orient="records")

    async def average_by_gender(self, program: str = None):
        df = await self.file_repository.load_csv()
        if program:
            df = df[df['PROGRAMA'] == program]
        male_avg = df[df['PENG_SEXO'] == 'M']['ESTP_PROMEDIOGENERAL'].mean()
        female_avg = df[df['PENG_SEXO'] == 'F']['ESTP_PROMEDIOGENERAL'].mean()
        return {"male_average": male_avg, "female_average": female_avg}

    async def average_by_program(self):
        df = await self.file_repository.load_csv()
        programs = df['PROGRAMA'].unique()
        results = [{"program": program, "program_average": df[df['PROGRAMA'] == program]['ESTP_PROMEDIOGENERAL'].mean()} for program in programs]
        return results

    async def gender_percentage(self, program: str = None):
        df = await self.file_repository.load_csv()
        if program:
            df = df[df['PROGRAMA'] == program]
        total = len(df)
        male_count = len(df[df['PENG_SEXO'] == 'M'])
        female_count = len(df[df['PENG_SEXO'] == 'F'])
        return {
            "program": program if program else "Todos los programas",
            "male_percentage": (male_count / total) * 100 if total > 0 else 0,
            "female_percentage": (female_count / total) * 100 if total > 0 else 0
        }

    async def average_age(self, program: str = None, by_gender: bool = True):
        df = await self.file_repository.load_csv()
        if program:
            df = df[df['PROGRAMA'] == program]
        if by_gender:
            male_avg_age = df[df['PENG_SEXO'] == 'M']['EDAD'].mean()
            female_avg_age = df[df['PENG_SEXO'] == 'F']['EDAD'].mean()
            return {"male_average_age": male_avg_age, "female_average_age": female_avg_age}
        else:
            return {"general_average_age": df['EDAD'].mean()}
