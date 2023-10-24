from app.net.bancodebogota.intermediate_functions import *
from app.net.bancodebogota.feature_functions import *
import argparse

#============ intermediate process =========================
#===========================================================
def intermediate_function(df,params):
    try:
        df2 = filtrar_prods(df , params)
        df2=fill_na(df2)
        df_types=definir_cols_types(df2, params)
        df_group=agrupacion_target(df_types, params)
        df_clean= cols_minuscula(df_group)

        return df_clean
    except Exception as e:
        print(f"Error in intermediate_function: {e}")
        raise

def main_intermediate(blob_path, blob_path_inter, output_path_intermediate):
    try:
        query_params = read_yaml_storage(blob_path)
    except FileNotFoundError:
        print(f"Error: .yml file not found at path: {blob_path}")
        return  # Exit the function since we can't proceed without the file.

    try:
        params = read_yaml_storage(blob_path_inter)
    except FileNotFoundError:
        print(f"Error: .yml file not found at path: {blob_path_inter}")
        return  # Exit the function since we can't proceed without the file.

    try:
        df = read_bq(query_params['train_data']['sql'])
        print(f"INFO: Master table read from Big Query, Dimension: {df.shape}")
    except Exception as e:
        print(f"Error while querying BigQuery: {e}")
        return

    df_clean = intermediate_function(df, params)

    try:
        df_to_storage_parquet(df_clean, output_path_intermediate)
    except Exception as e:
        print(f"Error while writing to Google Storage: {e}")

#================== Feature extract process ====================
#==============================================================
def feature_function(df,params):    

    df2 = calcular_edad(df, params['col_fecha_nacimiento'])
    df3 = calcular_generacion(df2,params['col_fecha_nacimiento'] )

    df_feature = calcular_grupo_etario(df3)

    return df_feature

def main_feature_processing(blob_path, path_intermediate, nombre_archivo_parquet, output_path):
    try:
        params = read_yaml_storage(blob_path)
    except FileNotFoundError:
        print(f"Error: .yml file not found at path: {blob_path}")
        return

    try:
        df_intermediate = read_parquet(path_intermediate)
        print(f"INFO: Intermediate parquet read from Google Storage, Dimension: {df_intermediate.shape}")
    except Exception as e:
        print(f"Error while reading the intermediate parquet: {e}")
        return

    if df_intermediate.empty:
        print("Error: No data found in the intermediate parquet file.")
        return

    df_feature = feature_function(df_intermediate, params)
    print("INFO: Features created successful")

    try:
        feature_to_storage_parquet(df_feature, output_path, nombre_archivo_parquet)
    except Exception as e:
        print(f"Error while writing features to Google Storage: {e}")
    
#================= main ======================================
#============================================================
def main(blob_path, blob_path_inter, output_path_intermediate, output_path):
    try:
        main_intermediate(blob_path, blob_path_inter, output_path_intermediate)
        print("Intermediate process successful")
        nombre_archivo_parquet = "feauture_persona_natural.parquet"
        main_feature_processing(blob_path, output_path_intermediate, nombre_archivo_parquet, output_path)
        print("Feature extraction process successful")
    except Exception as e:
        print(f"Unexpected error in the main process: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--blob_path', type=str, help='Path to the catalog.yml file')
    parser.add_argument('--blob_path_inter', type=str, help='Path intermediate.yml file')
    parser.add_argument('--output_path_intermediate', type=str, help='Final Path feature store')
    parser.add_argument('--output_path', type=str, help='Final Path feature store')
    args = parser.parse_args()
    main(args.blob_path, args.blob_path_inter, args.output_path_intermediate, args.output_path)    