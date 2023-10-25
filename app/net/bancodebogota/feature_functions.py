"""
funciones para el pipeline 'feature'
"""
import numpy as np
import pandas as pd
import gcsfs
import pyarrow.parquet as pq

from google.cloud import storage
import datetime
import os
from app.net.bancodebogota.globals import fc


def feature_to_storage_parquet(df, ruta_gcs, nombre_archivo_parquet):
    # Crear la fecha y hora actual en el formato deseado
    date_path = datetime.datetime.now().strftime("%d%m%Y%H:%M")

    # Construir la ruta completa del archivo
    full_path = f"{ruta_gcs.rstrip('/')}/{date_path}/{nombre_archivo_parquet}"

    # Escribir el DataFrame en la ruta utilizando el sistema de archivos de GCS
    df.to_parquet(full_path, index=False, storage_options={"token": "cloud"})
    fc.append_df("INFO", f"Feature DataFrame saved to storage as parquet: {full_path}")


def read_parquet(gs_path):
    """
    Reads a Parquet file from a GCS path and returns a Pandas DataFrame
    """
    fs = gcsfs.GCSFileSystem()

    # Open Parquet file using GCSFileSystem and ParquetFile
    with fs.open(gs_path, 'rb') as f:
        parquet_file = pq.ParquetFile(f)
        arrow_table = parquet_file.read()
        df = arrow_table.to_pandas()

    return df


def calcular_edad(df: pd.DataFrame, fecha_nacimiento: str) -> pd.DataFrame:
    """
    Calcular la edad del cliente de acuerdo a su fecha de nacimiento.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame donde se crearÃ¡ la nueva columna de edad.
    fecha_nacimiento : str
        Nombre de la columna con la fecha de nacimiento del cliente.

    Returns
    -------
    pd.DataFrame
        DataFrame con la nueva columna de edad del cliente.
    """
    # df["edad"] = (pd.Timestamp("now") - df[fecha_nacimiento]).dt.days // 365
    df["edad"] = (pd.Timestamp("now") - df[fecha_nacimiento]).astype("timedelta64[Y]")
    return df


def calcular_generacion(df: pd.DataFrame, fecha_nacimiento: str) -> pd.DataFrame:
    """
    Calcula la generaciÃ³n de acuerdo a fecha de nacimiento del cliente:

    - Si el ciente naciÃ³ antes de 1946, entonces serÃ¡ generaciÃ³n Tradicionalista.
    - Si el cliente naciÃ³ entre 1946 y 1965, entonces pertenece a la generaciÃ³n
      Baby Boomer.
    - Si el cliente naciÃ³ entre 1965 y 1980, entonces pertenece a la generaciÃ³n
      GeneraciÃ³n X.
    - Si el cliente naciÃ³ entre 1980 y 2000, entonces pertenece a la generaciÃ³n
      Millenial.
    - Si el cliente naciÃ³ despuÃ©s del aÃ±o 2000, entonces pertenece a la generaciÃ³n
      Centennial.


    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con fecha de nacimiento del cliente al que se desea agregar la
        generaciÃ³n.
    fecha_nacimiento : str
        Nombre de la columna con la fecha de nacimiento del cliente.

    Returns
    -------
    pd.DataFrame
        DataFrame con nueva columna de generaciÃ³n.
    """
    conds_gen = [
        df[fecha_nacimiento].dt.year >= 2000,
        (df[fecha_nacimiento].dt.year >= 1980) & (df[fecha_nacimiento].dt.year < 2000),
        (df[fecha_nacimiento].dt.year >= 1965) & (df[fecha_nacimiento].dt.year < 1980),
        (df[fecha_nacimiento].dt.year >= 1946) & (df[fecha_nacimiento].dt.year < 1965),
        df[fecha_nacimiento].dt.year < 1946,
    ]

    generacion = [
        "1_Centennial",
        "2_Millennial",
        "3_Generacion X",
        "4_Baby Boomer",
        "5_Tradicionalista",
    ]

    df["generacion"] = np.select(conds_gen, generacion)

    return df


def calcular_grupo_etario(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcular grupo etario de acuerdo a la edad del cliente:

    - Si el cliente es menor de 12 aÃ±os, serÃ¡ niÃ±o.
    - Si tiene entre 12 y 18 aÃ±os de edad, serÃ¡ adolescente.
    - Si tiene entre 18 y 27 aÃ±os de edad, serÃ¡ joven.
    - Si tiene entre 27 y 60 aÃ±os de edad, serÃ¡ adulto.
    - Si tiene 60 aÃ±os o mÃ¡s, serÃ¡ persona mayor.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame con fecha de nacimiento del cliente al que se desea agregar el
        grupo etario.

    Returns
    -------
    pd.DataFrame
        DataFrame con nueva columna de grupo etario.
    """
    rangos_etarios = [
        df["edad"] >= 60,
        (df["edad"] >= 27) & (df["edad"] < 60),
        (df["edad"] >= 18) & (df["edad"] < 27),
        (df["edad"] >= 12) & (df["edad"] < 18),
        df["edad"] < 12,
    ]

    grupo_etario = [
        "5_Persona Mayor",
        "4_Adulto",
        "3_Joven",
        "2_Adolescente",
        "1_Nino",
    ]

    df["grupo_etario"] = np.select(rangos_etarios, grupo_etario)

    return df

