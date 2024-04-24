import os
import json
import requests
import numpy as np
import pandas as pd
import random
from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, row_number,  col, when, rand
from pyspark.sql.window import Window
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, FloatType, NumericType
from funciones import generar_csv, generar_fechas, csv_to_parquet
import subprocess



def descargar_datos_covid(url_api, fecha):
    response = requests.get(url_api)
    
    if response.status_code == 200:
        datos_json = response.json()
        
        nombre_archivo = f"{fecha}.json"
        
        directorio = 'data'
        if not os.path.exists(directorio):
            os.makedirs(directorio)
        
        ruta_archivo = os.path.join(directorio, nombre_archivo)
        with open(ruta_archivo, 'w') as file:
            json.dump(datos_json, file, indent=4)
        
        print(f"Los datos de COVID descargados el {fecha}.json han sido guardados en '{ruta_archivo}'.")
    else:
        print("No se pudo descargar los datos. Estado de la solicitud:", response.status_code)

fecha = "20200602"
url_api = f"https://api.covidtracking.com/v1/us/{fecha}.json"

descargar_datos_covid(url_api, fecha)

spark = SparkSession.builder \
    .appName("Generar CSV") \
    .getOrCreate()

generar_csv(spark=spark)

def main():
    FOLDER_PATH = os.getcwd()
    daily_covid_path = os.path.join(FOLDER_PATH, "data", f"{fecha}.json")
    df = spark.read.option("multiline", True).json(daily_covid_path)
    df.createOrReplaceTempView("json")

    numeric_cols = [
        col.name for col in df.schema.fields if isinstance(col.dataType, NumericType)
    ]

    schema = StructType(
        [
            *[
                StructField(col_name, df.schema[col_name].dataType, True)
                for col_name in [*numeric_cols, "dateChecked"]
            ],
            StructField("id_city", IntegerType(), True),
            StructField("multiplication_factor", FloatType(), True),
        ]
    )

    try:
        rows_to_add = []
        for i in range(6):
            N_TIMES = 120
            city = i
            rows = df.select(*numeric_cols, "dateChecked").collect()[0]
            row = rows.asDict()
            dates = generar_fechas(date=row["date"], n_times=N_TIMES)
            to_row = {}

            for j in range(N_TIMES):
                m_factor = random.uniform(0.5, 1.6)
                for key in row:
                    match key:
                        case "date":
                            to_row[key] = dates[j]
                        case "dateChecked":
                            to_row[key] = row[key]
                        case _:
                            new_value = int(np.floor(row[key] * m_factor))
                            to_row[key] = new_value

                to_row["id_city"] = city+1
                to_row["multiplication_factor"] = m_factor
                rows_to_add.append(Row(**to_row))

        parquet_df = spark.createDataFrame(rows_to_add, schema)
        parquet_df.createOrReplaceTempView("parquet")
        parquet_df.toPandas().to_parquet("./data/covid_info.parquet")
        parquet_df.toPandas().to_csv("./data/covid_info.csv")
        print("Parquet saved, name: covid_info.parquet :)")
        
        csv_to_parquet(csv_path="../data/transformed_covid_data.csv")
    except Exception as e:
        print("Error while trying to save data into parquet file: {}".format(e))

if __name__ == "__main__":
    main()