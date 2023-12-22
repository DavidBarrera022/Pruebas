# globals.py
from .clLibFrameworkControl import ClLibFrameworkControl as Fc

fc = Fc()
app_name = "mlops598"
project_id ="bdb-gcp-qa-cds-idt"
fc.create_df(app_name, project_id)
