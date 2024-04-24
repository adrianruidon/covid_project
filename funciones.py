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

def generar_csv(spark):
    cities = [
        Row(
            id=1,
            country_id=1,
            name="Valencia",
            lat="39.47010514497293",
            lng="-0.3766213363909406",
        ),
        Row(
            id=2,
            country_id=2,
            name="París",
            lat="48.85663074697171",
            lng="2.3518663102905877",
        ),
        Row(
            id=3,
            country_id=3,
            name="New York",
            lat="40.7128",
            lng="-74.0060",
        ),
        Row(
            id=4, country_id=4, name="Tokyo", lat="35.6895", lng="139.6917"
        ),
        Row(
            id=5, country_id=5, name="London", lat="51.5074", lng="-0.1278"
        ),
        Row(
            id=6,
            country_id=6,
            name="Sydney",
            lat="-33.8743710663433",
            lng="151.0535114434631",
        ),
    ]

    countries = [
        Row(
            id=1,
            name="España",
        ),
        Row(
            id=2,
            name="Francia",
        ),
        Row(
            id=3,
            name="Estados Unidos",
        ),
        Row(id=4, name="Japón"),
        Row(id=5, name="Inglaterra"),
        Row(
            id=6,
            name="Australia",
        ),
    ]

    cities_df = spark.createDataFrame(
        cities,
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("country_id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("lat", StringType(), True),
                StructField("lng", StringType(), True),
            ]
        ),
    )

    countries_df = spark.createDataFrame(
        countries,
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        ),
    )

    # Creación de los archivos de ciudades.csv y paises.csv
    script_directory = os.getcwd()
    output_directory = os.path.join(script_directory, "data")

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    cities_output_path = os.path.join(output_directory, "ciudades.csv")
    countries_output_path = os.path.join(output_directory, "paises.csv")

    cities_pandas_df = cities_df.toPandas()
    countries_pandas_df = countries_df.toPandas()

    cities_pandas_df.to_csv(cities_output_path, index=False)
    countries_pandas_df.to_csv(countries_output_path, index=False)

def generar_fechas(date: int, n_times: int) -> List[int]:
    year = date // 10000
    month = (date // 100) % 100
    day = date % 100

    current_date = datetime(year, month, day)
    dates = []

    for _ in range(n_times):
        current_date += timedelta(days=1)
        dates.append(int(current_date.strftime('%Y%m%d')))
            
    return dates

def csv_to_parquet(csv_path: str | None = None, delimiter: str = ",") -> bool:
    try:
        if csv_path is None: 
            raise ValueError("CSV_PATH Cannot be None")
        
        splitted_path = csv_path.split("/")
        folder_path = "/".join(splitted_path[:-1])
        new_filename = splitted_path[-1].split(".")[0] + ".parquet"
        
        pd.read_csv(filepath_or_buffer=csv_path, delimiter=delimiter).to_parquet(path=f"{folder_path}/{new_filename}")
        print("Transformation Done :)")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False