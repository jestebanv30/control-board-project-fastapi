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

        # Leer el archivo según su tipo
        if file_extension.lower() == ".xlsx":
            temp_file_path = await self.file_repository.save_temp_file(file)
            df = pd.read_excel(temp_file_path)
        else:  # CSV
            temp_file_path = await self.file_repository.save_temp_file(file)
            df = pd.read_csv(temp_file_path)

        # Filtrar las columnas seleccionadas
        df_filtered = df[SELECTED_COLUMNS]

        # Guardar como archivo JSON
        json_file_path = self.file_repository.FILE_PATH.replace(".csv", ".json")
        df_filtered.to_json(json_file_path, orient='records', force_ascii=False, indent=4)

        return json_file_path
    
    async def list_all(self):
        # Cargar el archivo CSV
        df = await self.file_repository.load_data()

        # Reemplazar los NaN con un valor predeterminado o eliminarlos
        df = df.fillna(0)  # O puedes usar df.dropna() si prefieres eliminar filas con NaN

        # Convertir DataFrame a una lista de diccionarios y retornar
        return df.to_dict(orient='records')

    async def filter_by_program_and_avg(self, program: str, avg_threshold: float):
        try:
            # Cargar el archivo CSV
            df = await self.file_repository.load_data()

            # Rellenar NaN en la columna "ESTP_PROMEDIOGENERAL" con 0
            df["ESTP_PROMEDIOGENERAL"] = df["ESTP_PROMEDIOGENERAL"].fillna(0)

            # Convertir a float explícitamente si es necesario
            df["ESTP_PROMEDIOGENERAL"] = df["ESTP_PROMEDIOGENERAL"].astype(float)

            # Reemplazar infinitos por 0
            df = df.replace([float('inf'), float('-inf')], 0)

            # Filtrar por programa y promedio
            filtered_df = df[(df['PROGRAMA'] == program) & (df['ESTP_PROMEDIOGENERAL'] >= avg_threshold)]

            filtered_df = filtered_df.replace([float('inf'), float('-inf')], 0)  # Reemplazar infinitos con 0
            filtered_df = filtered_df.fillna(0)  # Reemplazar NaN con 0
            
            # Retorna el DataFrame filtrado
            return filtered_df.to_dict(orient='records')

        except KeyError as e:
            raise HTTPException(status_code=400, detail=f"Column not found: {str(e)}")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Data type error: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    async def average_by_gender(self, program: str = None):
        df = await self.file_repository.load_data()
        if program:
            df = df[df['PROGRAMA'] == program]
        male_avg = df[df['PENG_SEXO'] == 'M']['ESTP_PROMEDIOGENERAL'].mean()
        female_avg = df[df['PENG_SEXO'] == 'F']['ESTP_PROMEDIOGENERAL'].mean()
        return {"male_average": male_avg, "female_average": female_avg}

    async def average_by_program(self):
        df = await self.file_repository.load_data()
        programs = df['PROGRAMA'].unique()
        results = [{"program": program, "program_average": df[df['PROGRAMA'] == program]['ESTP_PROMEDIOGENERAL'].mean()} for program in programs]
        return results

    async def gender_percentage(self, program: str = None):
        df = await self.file_repository.load_data()
    
        if program:  # Si se proporciona un programa, filtrar el DataFrame
            df = df[df['PROGRAMA'] == program]
            total = len(df)
            male_count = len(df[df['PENG_SEXO'] == 'M'])
            female_count = len(df[df['PENG_SEXO'] == 'F'])
            return {
                "program": program,
                "male_percentage": (male_count / total) * 100 if total > 0 else 0,
                "female_percentage": (female_count / total) * 100 if total > 0 else 0,
            }
        else:  # Si no se proporciona programa, calcular para todos los programas y global
            # Calcular para cada programa
            programs = df['PROGRAMA'].unique()
            program_percentages = []
            for prog in programs:
                prog_df = df[df['PROGRAMA'] == prog]
                total_prog = len(prog_df)
                male_count = len(prog_df[prog_df['PENG_SEXO'] == 'M'])
                female_count = len(prog_df[prog_df['PENG_SEXO'] == 'F'])
                program_percentages.append({
                    "program": prog,
                    "male_percentage": (male_count / total_prog) * 100 if total_prog > 0 else 0,
                    "female_percentage": (female_count / total_prog) * 100 if total_prog > 0 else 0,
                })

            # Calcular el porcentaje global
            total = len(df)
            male_count = len(df[df['PENG_SEXO'] == 'M'])
            female_count = len(df[df['PENG_SEXO'] == 'F'])
            global_percentage = {
                "program": "Todos los programas",
                "male_percentage": (male_count / total) * 100 if total > 0 else 0,
                "female_percentage": (female_count / total) * 100 if total > 0 else 0,
            }

            # Combinar resultados
            return {
                "global": global_percentage,
                "by_program": program_percentages,
            }

    async def average_age(self, program: str = None, by_gender: bool = True):
        df = await self.file_repository.load_data()
    
        if program:  # Si se proporciona un programa específico
            df = df[df['PROGRAMA'] == program]
            if by_gender:
                male_avg_age = df[df['PENG_SEXO'] == 'M']['EDAD'].mean()
                female_avg_age = df[df['PENG_SEXO'] == 'F']['EDAD'].mean()
                return {
                    "program": program,
                    "male_average_age": male_avg_age if male_avg_age == male_avg_age else 0,  # Manejo de NaN
                    "female_average_age": female_avg_age if female_avg_age == female_avg_age else 0
                }
            else:
                general_avg_age = df['EDAD'].mean()
                return {
                    "program": program,
                    "general_average_age": general_avg_age if general_avg_age == general_avg_age else 0
                }
        else:  # Calcular para todos los programas y globalmente
            programs = df['PROGRAMA'].unique()
            results = []

            for prog in programs:
                prog_df = df[df['PROGRAMA'] == prog]
                if by_gender:
                    male_avg_age = prog_df[prog_df['PENG_SEXO'] == 'M']['EDAD'].mean()
                    female_avg_age = prog_df[prog_df['PENG_SEXO'] == 'F']['EDAD'].mean()
                    results.append({
                        "program": prog,
                        "male_average_age": male_avg_age if male_avg_age == male_avg_age else 0,
                        "female_average_age": female_avg_age if female_avg_age == female_avg_age else 0
                    })
                else:
                    general_avg_age = prog_df['EDAD'].mean()
                    results.append({
                        "program": prog,
                        "general_average_age": general_avg_age if general_avg_age == general_avg_age else 0
                    })

            # Calcular los promedios globales
            if by_gender:
                male_avg_age = df[df['PENG_SEXO'] == 'M']['EDAD'].mean()
                female_avg_age = df[df['PENG_SEXO'] == 'F']['EDAD'].mean()
                global_result = {
                    "program": "Todos los programas",
                    "male_average_age": male_avg_age if male_avg_age == male_avg_age else 0,
                    "female_average_age": female_avg_age if female_avg_age == female_avg_age else 0
                }
            else:
                general_avg_age = df['EDAD'].mean()
                global_result = {
                    "program": "Todos los programas",
                    "general_average_age": general_avg_age if general_avg_age == general_avg_age else 0
                }

            return {
                "global": global_result,
                "by_program": results
            }

    async def filter_by_document(self, document_id: int):
        try:
            # Cargar los datos desde el archivo JSON
            df = await self.file_repository.load_data()

            # Filtrar por documento de identidad
            filtered_df = df[df['PEGE_DOCUMENTOIDENTIDAD'] == document_id]

            # Manejar el caso en que no se encuentre el documento
            if filtered_df.empty:
                raise HTTPException(status_code=404, detail="Document ID not found.")

            # Reemplazar valores NaN e infinitos en el resultado
            filtered_df = filtered_df.replace([float('inf'), float('-inf')], None)
            filtered_df = filtered_df.fillna("")

            # Retornar los datos como lista de diccionarios
            return filtered_df.to_dict(orient='records')

        except HTTPException as http_exc:
            # Reenviar las excepciones HTTPException
            raise http_exc
        except Exception as e:
            # Cualquier otro error inesperado
            raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    async def socioeconomic_filters(self):
        try:
            # Cargar los datos
            df = await self.file_repository.load_data()
    
            total_students = len(df)
    
            # Contar estudiantes en cada categoría
            joven_accion_count = len(df[df['ESTP_JOVENACCION'] == 1])
            generacion_e_count = len(df[df['ESTP_GENERACION_E'] == 1])
            victima_count = len(df[df['ESTP_VICTIMA'] == 1])
            afro_count = len(df[df['ESTP_AFRO'] == 1])
    
            # Calcular porcentajes
            joven_accion_percentage = (joven_accion_count /     total_students) * 100 if total_students > 0 else 0
            generacion_e_percentage = (generacion_e_count /     total_students) * 100 if total_students > 0 else 0
            victima_percentage = (victima_count / total_students) *     100 if total_students > 0 else 0
            afro_percentage = (afro_count / total_students) * 100 if    total_students > 0 else 0
    
            # Retornar los resultados
            return {
                "total_students": total_students,
                "joven_accion": {
                    "count": joven_accion_count,
                    "percentage": joven_accion_percentage
                },
                "generacion_e": {
                    "count": generacion_e_count,
                    "percentage": generacion_e_percentage
                },
                "victima": {
                    "count": victima_count,
                    "percentage": victima_percentage
                },
                "afro": {
                    "count": afro_count,
                    "percentage": afro_percentage
                }
            }
        except KeyError as e:
            raise HTTPException(status_code=400, detail=f"Column not    found: {str(e)}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected    error: {str(e)}")
    