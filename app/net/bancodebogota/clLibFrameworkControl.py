import pandas as pd
from datetime import datetime
from google.cloud import storage
import pytz


class ClLibFrameworkControl:
    # Define the parameters
    def __init__(self):
        # self.bucket = bucket
        # self.file = file
        self.log_df = pd.DataFrame()
        self.name_app = ""

    # Create an empty DataFrame to store logs
    def create_df(self, name_app, proyect):
        try:
            self.name_app = name_app
            self.log_df = pd.DataFrame(columns=['TAG', 'DETALLE', 'FECHA'])

            ClLibFrameworkControl.append_df(self, "NOMBRE", name_app)
            ClLibFrameworkControl.append_df(self, "PROYECTO", proyect)
            ClLibFrameworkControl.append_df(self, "INFO", "Inicio Proceso")

            return 0, "Exitoso"
        except Exception as e:
            return 1, str(e)

    # Append DataFrame to store logs
    def append_df(self, log_type, message):
        try:
            current_time = datetime.now()
            data = {'TAG': str(log_type).upper(), 'DETALLE': message, 'FECHA': current_time}
            self.log_df = pd.concat([self.log_df, pd.DataFrame([data])], ignore_index=True)
        except Exception as e:
            print('Fatal error: ' + str(e))

    def create_file_bucket(self):
        try:
            hora_colombia = datetime.now(pytz.timezone('America/Bogota'))
            hora_colombiana_hora_atras = pd.to_datetime(hora_colombia)
            fecha_convert_colombiana = hora_colombiana_hora_atras.strftime('%Y%m%d_%H%M%S')

            bucket = 'bdb-gcp-st-cds-idt-frm-control-zone'
            file = self.name_app + '_' + fecha_convert_colombiana + '.log'
            path_file = 'logs/' + file
            path_local = "/tmp/" + file

            # Conventioneer Dataframe to Pandas
            self.log_df.to_csv(path_local, index=False, header=True, sep="|")

            # Get Google Cloud client
            client = storage.Client()
            bucket = client.bucket(bucket)
            data_blob = bucket.blob(path_file)

            # Create file & Write file in Buckets
            data_blob.upload_from_filename(path_local)

        except Exception as e:
            print('Fatal error: ' + str(e))