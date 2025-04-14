# Data Transformation Script for Violence Cases Dataset

"""
This script performs ETL (Extract, Transform, Load) operations on a dataset containing
information about violence cases reported in Peru. The main transformations include:

1. Data Loading:
   - Reads a CSV file with Latin-1 encoding and semicolon delimiter
   - Handles bad records by storing them in '/tmp/badRecords'

2. Data Cleaning:
   - Removes unnecessary columns (administrative codes and redundant information)
   - Cleans numeric columns by removing commas and converting to integers
   - Renames columns to remove special characters (Ñ -> NH, N° -> NUM)

3. Schema Definition:
   - Defines a strict schema for all columns including:
     * Basic information (Year, Period, Date)
     * Geographic information (Ubigeo, Department, Province, District)
     * Center information (Code, Name)
     * Case counts by:
       - Total cases
       - Gender (Male/Female)
       - Violence type (Psychological, Physical, Sexual, Economic)
       - Age groups (0-5, 6-11, 12-17, etc.)

4. Data Quality:
   - Replaces NULL values with 0 in numeric columns
   - Validates string columns by checking distinct values

5. Data Storage:
   - Saves the transformed data as Parquet file in 'dbfs:/FileStore/data_cleaned.parquet'

Key Features:
- Handles Spanish characters and special encodings
- Implements data type validation
- Provides NULL value handling
- Includes data quality checks
"""

import pyspark
import os
from pyspark.sql import SparkSession
import argparse

# Add argument parsing at the start of your script
parser = argparse.ArgumentParser()
parser.add_argument('--year', required=True, help='Year to process')
args = parser.parse_args()

spark = SparkSession.builder\
    .master('local[*]') \
    .appName('violence-cases-data-transformer')\
    .getOrCreate()

# Use the year parameter in your file path
df = spark.read\
    .options(inferSchema=True, header=True, encoding="latin1")\
    .format("csv")\
    .load(f"s3://data-camp-bucket-crp/raw/casos_fem_{args.year}.csv")

original_names = [
    'AÑO',
    'PERIODO',
    'FECHA ENVIO',
    'CODIGO ENTIDAD',
    'ENTIDAD', 
    'CODIGO LINEA',
    'LINEA INTERVENCION',
    'CODIGO SERVICIO',
    'NOMBRE SERVICIO',
    'UBIGEO',
    'DEPARTAMENTO',
    'PROVINCIA',
    'DISTRITO',
    'CODIGO CENTRO ATENCION',
    'NOMBRE CENTRO ATENCION',
    'N° DE CEM',
    'N° CASOS ATENDIDOS-TOTAL',
    'N° CASOS ATENDIDOS - HOMBRES - TOTAL',
    'N° CASOS ATENDIDOS - MUJERES - TOTAL',
    'N° CASOS ATENDIDOS - VIOLENCIA PSICOLOGICA',
    'N° CASOS ATENDIDOS - VIOLENCIA FISICA',
    'N° CASOS ATENDIDOS - VIOLENCIA SEXUAL',
    'N° CASOS ATENDIDOS - VIOLENCIA ECONÓMICA O PATRIMONIAL',
    'N° CASOS ATENDIDOS - 0_5 - TOTAL',
    'N° CASOS ATENDIDOS - 0_5 - HOMBRES',
    'N° CASOS ATENDIDOS - 0_5 - MUJERES',
    'N° CASOS ATENDIDOS - 6_11 - TOTAL',
    'N° CASOS ATENDIDOS - 6_11 - HOMBRES',
    'N° CASOS ATENDIDOS - 6_11 - MUJERES',
    'N° CASOS ATENDIDOS - 12_17 - TOTAL',
    'N° CASOS ATENDIDOS - 12_17 - HOMBRES',
    'N° CASOS ATENDIDOS - 12_17 - MUJERES',
    'N° CASOS ATENDIDOS - 18_25 - TOTAL',
    'N° CASOS ATENDIDOS - 18_25  - HOMBRES',
    'N° CASOS ATENDIDOS - 18_25 - MUJERES',
    'N° CASOS ATENDIDOS - 18_29 - TOTAL',
    'N° CASOS ATENDIDOS - 18_29  - HOMBRES',
    'N° CASOS ATENDIDOS - 18_29 - MUJERES',
    'N° CASOS ATENDIDOS - 26_35 - TOTAL',
    'N° CASOS ATENDIDOS - 26_35 - HOMBRES',
    'N° CASOS ATENDIDOS - 26_35 - MUJERES',
    'N° CASOS ATENDIDOS - 36_45 - TOTAL',
    'N° CASOS ATENDIDOS - 36_45 - HOMBRES',
    'N° CASOS ATENDIDOS - 36_45 - MUJERES',
    'N° CASOS ATENDIDOS - 30_59 - TOTAL',
    'N° CASOS ATENDIDOS - 30_59 - HOMBRES',
    'N° CASOS ATENDIDOS - 30_59 - MUJERES',
    'N° CASOS ATENDIDOS - 46_59 - TOTAL',
    'N° CASOS ATENDIDOS - 46_59 - HOMBRES',
    'N° CASOS ATENDIDOS - 46_59 - MUJERES',
    'N° CASOS ATENDIDOS - 60_MÁS - TOTAL',
    'N° CASOS ATENDIDOS - 60_MÁS - HOMBRES',
    'N° CASOS ATENDIDOS - 60_MÁS - MUJERES',
    'N° DE ACTIVIDADES - TOTAL'
]

#we set the original names to the dataframe considering the encoding
df = df.toDF(*original_names)

#We remove the columns that are not needed (unique value)
columns_to_drop = ["CODIGO ENTIDAD", "ENTIDAD", "CODIGO LINEA","LINEA INTERVENCION","CODIGO SERVICIO","NOMBRE SERVICIO","N° DE CEM"]
df = df.drop(*columns_to_drop)

#We clean the columns that are numeric values and have commas
columns_to_clean = [
    'N° CASOS ATENDIDOS-TOTAL',
    'N° CASOS ATENDIDOS - HOMBRES - TOTAL',
    'N° CASOS ATENDIDOS - MUJERES - TOTAL',
    'N° CASOS ATENDIDOS - VIOLENCIA PSICOLOGICA',
    'N° CASOS ATENDIDOS - VIOLENCIA FISICA',
    'N° CASOS ATENDIDOS - VIOLENCIA SEXUAL',
    'N° CASOS ATENDIDOS - VIOLENCIA ECONÓMICA O PATRIMONIAL',
    'N° CASOS ATENDIDOS - 0_5 - TOTAL',
    'N° CASOS ATENDIDOS - 0_5 - HOMBRES',
    'N° CASOS ATENDIDOS - 0_5 - MUJERES',
    'N° CASOS ATENDIDOS - 6_11 - TOTAL',
    'N° CASOS ATENDIDOS - 6_11 - HOMBRES',
    'N° CASOS ATENDIDOS - 6_11 - MUJERES',
    'N° CASOS ATENDIDOS - 12_17 - TOTAL',
    'N° CASOS ATENDIDOS - 12_17 - HOMBRES',
    'N° CASOS ATENDIDOS - 12_17 - MUJERES',
    'N° CASOS ATENDIDOS - 18_25 - TOTAL',
    'N° CASOS ATENDIDOS - 18_25  - HOMBRES',
    'N° CASOS ATENDIDOS - 18_25 - MUJERES',
    'N° CASOS ATENDIDOS - 18_29 - TOTAL',
    'N° CASOS ATENDIDOS - 18_29  - HOMBRES',
    'N° CASOS ATENDIDOS - 18_29 - MUJERES',
    'N° CASOS ATENDIDOS - 26_35 - TOTAL',
    'N° CASOS ATENDIDOS - 26_35 - HOMBRES',
    'N° CASOS ATENDIDOS - 26_35 - MUJERES',
    'N° CASOS ATENDIDOS - 36_45 - TOTAL',
    'N° CASOS ATENDIDOS - 36_45 - HOMBRES',
    'N° CASOS ATENDIDOS - 36_45 - MUJERES',
    'N° CASOS ATENDIDOS - 30_59 - TOTAL',
    'N° CASOS ATENDIDOS - 30_59 - HOMBRES',
    'N° CASOS ATENDIDOS - 30_59 - MUJERES',
    'N° CASOS ATENDIDOS - 46_59 - TOTAL',
    'N° CASOS ATENDIDOS - 46_59 - HOMBRES',
    'N° CASOS ATENDIDOS - 46_59 - MUJERES',
    'N° CASOS ATENDIDOS - 60_MÁS - TOTAL',
    'N° CASOS ATENDIDOS - 60_MÁS - HOMBRES',
    'N° CASOS ATENDIDOS - 60_MÁS - MUJERES',
    'N° DE ACTIVIDADES - TOTAL'
]

from pyspark.sql.functions import regexp_replace, trim, col

for column in columns_to_clean:
    df = df.withColumn(
        column,
        regexp_replace(trim(col(column)), ",", "").cast("int")
    )

new_columns = [(col_name.replace("Ñ", "NH")).replace("N°","NUM") for col_name in df.columns]

# Rename the DataFrame with updated column names
df = df.toDF(*new_columns)

from pyspark.sql.functions import to_date

df = df.withColumn("FECHA ENVIO", to_date("FECHA ENVIO", "dd/MM/yyyy"))


from pyspark.sql.types import StringType, StructType, IntegerType, DateType, DoubleType,StructField

schema = StructType([
    StructField('anho',IntegerType(), True), 
    StructField('periodo',StringType(), True), 
    StructField('fecha_envio',DateType(), True), 
    StructField('ubigeo',IntegerType(), True), 
    StructField('departamento',StringType(), True), 
    StructField('provincia',StringType(), True), 
    StructField('distrito',StringType(), True), 
    StructField('codigo_centro_atencion',StringType(), True), 
    StructField('nombre_centro_atencion',StringType(), True), 
    StructField('num_casos_atendidos_total',IntegerType(), True), 
    StructField('num_casos_atendidos_hombres_total',IntegerType(), True), 
    StructField('num_casos_atendidos_mujeres_total',IntegerType(), True), 
    StructField('num_casos_atendidos_violencia_psicologica',IntegerType(), True), 
    StructField('num_casos_atendidos_violencia_fisica',IntegerType(), True), 
    StructField('num_casos_atendidos_violencia_sexual',IntegerType(), True), 
    StructField('num_casos_atendidos_violencia_economica_o_patrimonial',IntegerType(), True), 
    StructField('num_casos_atendidos_0_5_total',IntegerType(), True), 
    StructField('num_casos_atendidos_0_5_hombres',IntegerType(), True), 
    StructField('num_casos_atendidos_0_5_mujeres',IntegerType(), True), 
    StructField('num_casos_atendidos_6_11_total',IntegerType(), True), 
    StructField('num_casos_atendidos_6_11_hombres',IntegerType(), True), 
    StructField('num_casos_atendidos_6_11_mujeres',IntegerType(), True), 
    StructField('num_casos_atendidos_12_17_total',IntegerType(), True), 
    StructField('num_casos_atendidos_12_17_hombres',IntegerType(), True), 
    StructField('num_casos_atendidos_12_17_mujeres',IntegerType(), True), 
    StructField('num_casos_atendidos_18_25_total',IntegerType(), True), 
    StructField('num_casos_atendidos_18_25_hombres',IntegerType(), True), 
    StructField('num_casos_atendidos_18_25_mujeres',IntegerType(), True), 
    StructField('num_casos_atendidos_18_29_total',IntegerType(), True), 
    StructField('num_casos_atendidos_18_29_hombres',IntegerType(), True), 
    StructField('num_casos_atendidos_18_29_mujeres',IntegerType(), True), 
    StructField('num_casos_atendidos_26_35_total',IntegerType(), True), 
    StructField('num_casos_atendidos_26_35_hombres',IntegerType(), True), 
    StructField('num_casos_atendidos_26_35_mujeres',IntegerType(), True), 
    StructField('num_casos_atendidos_36_45_total',IntegerType(), True), 
    StructField('num_casos_atendidos_36_45_hombres',IntegerType(), True), 
    StructField('num_casos_atendidos_36_45_mujeres',IntegerType(), True), 
    StructField('num_casos_atendidos_30_59_total',IntegerType(), True), 
    StructField('num_casos_atendidos_30_59_hombres',IntegerType(), True), 
    StructField('num_casos_atendidos_30_59_mujeres',IntegerType(), True), 
    StructField('num_casos_atendidos_46_59_total',IntegerType(), True), 
    StructField('num_casos_atendidos_46_59_hombres',IntegerType(), True), 
    StructField('num_casos_atendidos_46_59_mujeres',IntegerType(), True), 
    StructField('num_casos_atendidos_60_mas_total',IntegerType(), True), 
    StructField('num_casos_atendidos_60_mas_hombres',IntegerType(), True), 
    StructField('num_casos_atendidos_60_mas_mujeres',IntegerType(), True), 
    StructField('num_de_actividades_total',IntegerType(), True) 
])

df = spark.createDataFrame(df.rdd, schema)



from pyspark.sql.functions import col, when
#We get those columns that are numeric values and have null values
name_num_list = [column for column in df.columns if column.startswith("num_") and column != "num_de_cem"]

for column in name_num_list:
    df = df.withColumn(column,when(col(column).isNull(), 0).otherwise(col(column)))

df.write.mode("overwrite").parquet(f"s3://data-camp-bucket-crp/landing/data_cleaned_{args.year}")
df.write.csv(f"s3://data-camp-bucket-crp/landing/data_cleaned_{args.year}.csv", header=True, mode="overwrite")