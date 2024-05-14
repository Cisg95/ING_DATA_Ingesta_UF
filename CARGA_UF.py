import os
import requests
import pandas as pd
from pandas_gbq import read_gbq
from pandas_gbq import to_gbq

# Configurar variables para API
''' API_KEY, la key se mantiene activa mientras exista el usuario cissavedrag@falabella.cl. 
    Si el usuario deja de existir, revisar la documentacion: https://api.cmfchile.cl/documentacion/UF.html para obtener una nueva
'''
API_URL = 'https://api.cmfchile.cl/api-sbifv3/recursos_api/uf?apikey='
API_KEY = os.environ["API_KEY"]
API_FORMAT = '&formato=json'

# Variables del proyecto GCP
TABLA_DESTINO = 'operaciones_payment.uf_table'
PROYECTO = 'fif-pay-cross-development'

def main():
    # Obtener datos de la API
    data = requests.get(API_URL + API_KEY + API_FORMAT)
    json_uf = data.json()

    valor_uf = float(json_uf['UFs'][0]['Valor'].replace('.','').replace(',','.'))
    fecha = json_uf['UFs'][0]['Fecha']
    
    # Verificar si hoy existe data
    QUERY = f"SELECT FECHA FROM `{PROYECTO}.{TABLA_DESTINO}` WHERE FECHA = DATE '{fecha}'"
    QUERY_RESULT = read_gbq(QUERY,project_id=PROYECTO,progress_bar_type= None)
    
    if QUERY_RESULT.empty:
        # Crear un DataFrame con los datos
        df = pd.DataFrame({'FECHA': [fecha], 'VALOR_UF': [valor_uf]})
        df['FECHA'] = pd.to_datetime(df['FECHA'])

        # Escribir el DataFrame en BigQuery
        to_gbq(df, destination_table=TABLA_DESTINO, project_id=PROYECTO, if_exists='append')
    else:
        print('Ya cuenta con data para el dia de hoy')

if __name__ == "__main__":
    main()