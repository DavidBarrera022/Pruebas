from intermediate_functions import *
from feature_functions import *
import argparse
from globals import fc

def handle_error_and_create_bucket(error_msg, e=None):
    fc.append_df("ERROR", f"{error_msg}: {e}" if e else error_msg)
    fc.create_file_bucket()

def intermediate_function(df, params):
    try:
        #df2 = filtrar_prods(df , params)
        df2 = fill_na(df)
        df_types = definir_cols_types(df2, params)
        df_group = agrupacion_target(df_types, params)
        df_clean = cols_minuscula(df_group)
        return df_clean
    except Exception as e:
        handle_error_and_create_bucket("Error in intermediate_function", e)

def main_intermediate(blob_path, blob_path_inter, output_path_intermediate):
    try:
        query_params = read_yaml_storage(blob_path)
        params = read_yaml_storage(blob_path_inter)
        df = read_bq(query_params['train_data']['sql'])
        fc.append_df("INFO", f"Master table read from Big Query, Dimension: {df.shape}")
        print(f"INFO Master table read from Big Query, Dimension: {df.shape}")
        df_clean = intermediate_function(df, params)
        df_to_storage_parquet(df_clean, output_path_intermediate)
    except FileNotFoundError as e:
        if "blob_path" in str(e):
            handle_error_and_create_bucket(f".yml file not found at path", e)
        elif "blob_path_inter" in str(e):
            handle_error_and_create_bucket(f".yml file not found at path", e)
    except Exception as e:
        handle_error_and_create_bucket("Unexpected error in main_intermediate", e)

def feature_function(df, params):
    df2 = calcular_edad(df, params['col_fecha_nacimiento'])
    df3 = calcular_generacion(df2, params['col_fecha_nacimiento'])
    df_feature = calcular_grupo_etario(df3)
    return df_feature

def main_feature_processing(blob_path, path_intermediate, nombre_archivo_parquet, output_path):
    try:
        params = read_yaml_storage(blob_path)
        df_intermediate = read_parquet(path_intermediate)
        fc.append_df("INFO", f"Intermediate parquet read from Google Storage, Dimension: {df_intermediate.shape}")
        print(f"INFO Intermediate parquet read from Google Storage, Dimension: {df_intermediate.shape}")
        if df_intermediate.empty:
            handle_error_and_create_bucket("No data found in the intermediate parquet file.")
        df_feature = feature_function(df_intermediate, params)
        fc.append_df("INFO", "Features created successful")
        print("INFO Features created successful")
        feature_to_storage_parquet(df_feature, output_path, nombre_archivo_parquet)
    except FileNotFoundError as e:
        handle_error_and_create_bucket(f".yml file not found at path", e)
    except Exception as e:
        handle_error_and_create_bucket("Unexpected error in main_feature_processing", e)

def main(blob_path, blob_path_inter, output_path_intermediate, output_path):
    try:
        main_intermediate(blob_path, blob_path_inter, output_path_intermediate)
        fc.append_df("INFO", "Intermediate process successful")
        print("INFO Intermediate process successful")
        nombre_archivo_parquet = "feauture_persona_natural.parquet"
        main_feature_processing(blob_path, output_path_intermediate, nombre_archivo_parquet, output_path)
        fc.append_df("INFO", "Feature extraction process successful")
        fc.create_file_bucket()
        print("INFO Feature extraction process successful")
    except Exception as e:
        handle_error_and_create_bucket("Unexpected error in the main process", e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--blob_path', type=str, help='Path to the catalog.yml file')
    parser.add_argument('--blob_path_inter', type=str, help='Path intermediate.yml file')
    parser.add_argument('--output_path_intermediate', type=str, help='Final Path feature store')
    parser.add_argument('--output_path', type=str, help='Final Path feature store')
    args = parser.parse_args()
    main(args.blob_path, args.blob_path_inter, args.output_path_intermediate, args.output_path)
