"""
Funciones etapa intermedia construccion feature store 598
"""
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from typing import Dict, List, Any
import yaml
from io import BytesIO
from app.net.bancodebogota.globals import fc


def read_bq(query):
    """
    lectura tabla from BQ.
    Se hace laconsulta mediante un query y se almacena la data en un pd.DataFrame
    """

    client = bigquery.Client()
    df = client.query(query).to_dataframe()

    return df


def filtrar_prods(df: pd.DataFrame, params: Dict[Any, Any]) -> pd.DataFrame:
    """
    Filtrar productos que estÃ¡n en el alcance del entrenamiento del modelo.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame consultado desde la fuente.
    params : Dict[Any, Any]
        Diccionario de parÃ¡metros.

    Returns
    -------
    pd.DataFrame
        DataFrame filtrado.
    """
    df = df.loc[df[params['producto_agrupado_col']].isin(params['products'])]

    fc.append_df("INFO", f"Shape Dataframe after filter by products: {df.shape}")

    return df


def fill_na(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reemplazar string de 'NA' con numpy.nan.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame al que se desea reemplazar los strings.

    Returns
    -------
    pd.DataFrame
        DataFrame con numpy.nan.
    """
    df = df.fillna(np.nan)

    return df


def definir_cols_types(
        df: pd.DataFrame, params: Dict[Any, Any]
) -> pd.DataFrame:
    """
    Cambiar los tipos de columna de acuerdo al diccionario (Dict[col, type]).
    A las columnas de fechas especificadas se les transforma a Datetime.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame al que se le desea ajustar los tipos de columna.
    params : Dict[Any, Any]
        Diccionario de parÃ¡metros.

    Returns
    -------
    pd.DataFrame
        DataFrame con tipos de columnas ajustados.
    """
    # Cambiar tipos de datos
    df = df.astype(params['columns_type'])

    # Cambiar tipos de columnas de fecha a datetime
    for col in params['date_columns']:
        df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)

    return df


def agrupacion_target(df: pd.DataFrame, params: Dict[Any, Any]) -> pd.DataFrame:
    """
    Ajustar la agrupacion de productos para que se refleje Adelanto de Nomina y
    Crediservice de manera correcta.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame al que se le desea ajustar los tipos de columna.
    params : Dict[Any, Any]
        Diccionario de parÃ¡metros.

    Returns
    -------
    pd.DataFrame
        DataFrame agrupacion de productos ajustada.
    """

    df[params['producto_agrupado_col']] = np.where(df[params['codigo_linea_col']] == "110",
                                                   "ADN",
                                                   df[params['producto_agrupado_col']])

    df[params['producto_agrupado_col']] = np.where(df[params['producto_agrupado_col']] == "Rotativos",
                                                   "Crediservice",
                                                   df[params['producto_agrupado_col']])

    df = df.drop(columns=[params['codigo_linea_col']])

    return df


def definir_index(df: pd.DataFrame, params: Dict[Any, Any]) -> pd.DataFrame:
    """
    Definir como index las columnas espeficificadas en id_cols.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame al que se le desea establecer un index.
    params : Dict[Any, Any]
        Diccionario de parÃ¡metros.

    Returns
    -------
    pd.DataFrame
        DataFrame con index asignado.
    """
    df = df.set_index(params['id_column'])

    return df


def cols_minuscula(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandarizar nombres de columnas en minÃºscula.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame al que se desea ajustar el nombre de las columnas.

    Returns
    -------
    pd.DataFrame
        DataFrame con nombres de columnas ajustados.
    """
    df.columns = df.columns.str.lower()

    return df


# read a yaml file from gogole storage
def read_yaml_storage(blob_path):
    # Create a client to interact with GCS
    client = storage.Client()
    # Split the blob path to get the bucket and blob names
    bucket_name, blob_name = blob_path.replace('gs://', '').split('/', 1)
    # Get the bucket and blob objects
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Download the YAML file as a string
    yaml_data = blob.download_as_text()
    # Parse the YAML data
    query_params = yaml.safe_load(yaml_data)

    return query_params


def df_to_storage_parquet(df, ruta_gcs):
    # Parse GCS path
    bucket_name, blob_name = ruta_gcs.replace('gs://', '').split('/', 1)

    # Initialize GCS client
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Create a BytesIO object and write the DataFrame to it using Parquet format
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)

    # Upload BytesIO object to GCS
    buffer.seek(0)
    blob.upload_from_file(buffer, content_type='application/octet-stream')
    fc.append_df("INFO", f"DataFrame saved to storage as parquet: {ruta_gcs}")