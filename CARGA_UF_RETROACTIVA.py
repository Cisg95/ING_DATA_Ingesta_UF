import requests
from datetime import datetime, timedelta
import pandas as pd
from pandas_gbq import to_gbq

def main():
    # Configurar variables para API
    ''' API_KEY, la key se mantiene activa mientras exista el usuario cissavedrag@falabella.cl. 
        Si el usuario deja de existir, revisar la documentacion: https://api.cmfchile.cl/documentacion/UF.html para obtener una nueva
    '''
    API_URL = 'https://api.cmfchile.cl/api-sbifv3/recursos_api/uf/'
    API_KEY = 'c4d1fe42dc72782d5858092c7eb57ab66ec327b5'
    API_FORMAT = '&formato=json'

    TABLA_DESTINO = 'operaciones_payment.uf_table'
    PROYECTO = 'fif-pay-cross-development'

    # Ajustar el año, mes y día en la URL de la API
    api_url_template = "{api_url}{year}/{month}/dias/{day}?apikey={api_key}{api_format}"

    # Definir el año, mes y día para los cuales queremos obtener los datos de UF
    year = 2024
    month = 4
    day = 30

    # Obtener la fecha inicial y final para el rango de fechas
    fecha_inicial = datetime(year, month, day).date()
    fecha_final = datetime.now().date()

    # Iterar sobre cada día en el rango de fechas
    fecha = fecha_inicial
    while fecha <= fecha_final:
        # Construir la URL de la API con el año, mes y día específicos
        api_url = api_url_template.format(api_url= API_URL, year=fecha.year, month=fecha.month, day=fecha.day, api_key = API_KEY, api_format= API_FORMAT)

        # Obtener datos de la API
        data = requests.get(api_url)

        # Verificar si la solicitud fue exitosa
        if data.status_code == 200:
            json_uf = data.json()
            valor_uf = float(json_uf['UFs'][0]['Valor'].replace('.','').replace(',','.'))
            fecha_str = json_uf['UFs'][0]['Fecha']
            
            # Crear DataFrame con los datos
            df = pd.DataFrame({'FECHA': [fecha_str], 'VALOR_UF': [valor_uf]})
            df['FECHA'] = pd.to_datetime(df['FECHA'])
            
            # Escribir el DataFrame en BigQuery
            to_gbq(df, destination_table=TABLA_DESTINO, project_id=PROYECTO, if_exists='append')

            # Incrementar la fecha en un día
            fecha += timedelta(days=1)
        else:
            print(f"No se pudieron obtener datos para la fecha {fecha}.")

if __name__ == "__main__":
    main()
