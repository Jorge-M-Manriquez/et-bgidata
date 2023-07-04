import requests
import pandas as pd
from google.cloud import storage
from datetime import datetime
import os
import shutil

client = storage.Client()
bucket_name = 'data_realtime'

class ParaderosIdaError(Exception):
    pass

class ParaderosRegresoError(Exception):
    pass

class HorariosIdaError(Exception):
    pass

class HorariosRegresoError(Exception):
    pass

def obtener_datos(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print('Error al obtener los datos de la API.')
        return None
    
def guardar_error(cod_url, error_msg, folder_name):
    error_data = {
        'recorrido': cod_url,
        'error_msg': error_msg,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    error_file_name = f'error_{folder_name}.csv'
    error_df = pd.DataFrame([error_data])
    error_df.to_csv(error_file_name, index=False, mode='a', header=not os.path.exists(error_file_name))

    print(f'Error del recorrido {cod_url} guardado en el archivo {error_file_name}.')

def procesar_datos_recorrido(rec_data, cod_url, folder_name):
    paraderos_df = pd.DataFrame(columns=['recorrido', 'trayecto', 'name', 'comuna', 'latitud', 'longitud'])
    horarios_df = pd.DataFrame(columns=['recorrido', 'trayecto', 'tipoDia', 'inicio', 'fin'])
    error = False
    try:
        if 'ida' in rec_data and 'paraderos' in rec_data['ida']:
            horarios_ida = rec_data['ida']['horarios']
            horarios_ida_df = pd.DataFrame(horarios_ida, columns=['tipoDia', 'inicio', 'fin'])
            horarios_ida_df['trayecto'] = 'ida'
            horarios_ida_df['recorrido'] = cod_url
            horarios_df = pd.concat([horarios_df, horarios_ida_df], ignore_index=True)
        else:
            error = True
            raise HorariosIdaError('No se encontraron horarios de ida.')
        
        if 'ida' in rec_data and 'paraderos' in rec_data['ida']:
            paraderos_ida = rec_data['ida']['paraderos']
            paraderos_ida_df = pd.DataFrame(paraderos_ida, columns=['name', 'comuna'])
            paraderos_ida_df['trayecto'] = 'ida'
            paraderos_ida_df['recorrido'] = cod_url
            for paradero in paraderos_ida:
                if 'pos' in paradero:
                    pos_x = paradero['pos'][0]
                    pos_y = paradero['pos'][1]
            paraderos_ida_df['latitud'] = pos_x
            paraderos_ida_df['longitud'] = pos_y
            paraderos_df = pd.concat([paraderos_df, paraderos_ida_df], ignore_index=True)
        else:
            error = True
            raise ParaderosIdaError('No se encontraron paraderos de ida.')
        
        if 'regreso' in rec_data and 'horarios' in rec_data['regreso']:
            horarios_regreso = rec_data['regreso']['horarios']
            horarios_regreso_df = pd.DataFrame(horarios_regreso, columns=['tipoDia', 'inicio', 'fin'])
            horarios_regreso_df['trayecto'] = 'regreso'
            horarios_regreso_df['recorrido'] = cod_url
            horarios_df = pd.concat([horarios_df, horarios_regreso_df], ignore_index=True)
        else:
            error = True
            raise HorariosRegresoError('No se encontraron horarios de regreso.')
        
        if 'regreso' in rec_data and 'paraderos' in rec_data['regreso']:
            paraderos_regreso = rec_data['regreso']['paraderos']
            paraderos_regreso_df = pd.DataFrame(paraderos_regreso, columns=['name', 'comuna'])
            paraderos_regreso_df['trayecto'] = 'regreso'
            paraderos_regreso_df['recorrido'] = cod_url
            for paradero in paraderos_regreso:
                if 'pos' in paradero:
                    pos_x = paradero['pos'][0]
                    pos_y = paradero['pos'][1]
            paraderos_regreso_df['latitud'] = pos_x
            paraderos_regreso_df['longitud'] = pos_y
            paraderos_df = pd.concat([paraderos_df, paraderos_regreso_df], ignore_index=True)
        else:
            error = True
            raise ParaderosRegresoError('No se encontraron paraderos de regreso.')
        
        if not error:
            print(f'Datos del recorrido {cod_url} procesados.')
            return paraderos_df, horarios_df
    except (HorariosIdaError, ParaderosIdaError, HorariosRegresoError, ParaderosRegresoError) as e:
        print(f'Error al procesar los datos del recorrido {cod_url}: {str(e)}')
        guardar_error(cod_url, str(e), folder_name)

    if not error:
        print(f'Datos del recorrido {cod_url} procesados.')
        return paraderos_df, horarios_df
    else:
        return None, None

def obtener_url_final(cod_rec):
    return f'https://www.red.cl/restservice_v2/rest/conocerecorrido?codsint={cod_rec}'

def guardar_datos(data, folder_name):

    paraderos_df = pd.DataFrame(columns=['recorrido', 'trayecto', 'name', 'comuna', 'latitud', 'longitud'])
    horarios_df = pd.DataFrame(columns=['recorrido', 'trayecto', 'tipoDia', 'inicio', 'fin'])

    paraderos_df_list = []
    horarios_df_list = []
    error_df_list = []

    for rec_data in data:
        if isinstance(rec_data, str):
            cod_url = rec_data.split('=')[-1]
            url = obtener_url_final(cod_url)
            rec_response = obtener_datos(url)

            if rec_response is not None:
                if isinstance(rec_response, list):
                    for rec_data in rec_response:
                        paraderos, horarios = procesar_datos_recorrido(rec_data, cod_url, folder_name)
                        if paraderos is not None and horarios is not None:
                            paraderos_df_list.append(paraderos)
                            horarios_df_list.append(horarios)
                        else:
                            error_df_list.append(pd.DataFrame([[cod_url, "Error al procesar los datos del recorrido"]], columns=['recorrido', 'error_msg']))
                elif isinstance(rec_response, dict):
                    paraderos, horarios = procesar_datos_recorrido(rec_response, cod_url, folder_name)
                    if paraderos is not None and horarios is not None:
                        paraderos_df_list.append(paraderos)
                        horarios_df_list.append(horarios)
                    else:
                        error_df_list.append(pd.DataFrame([[cod_url, "Error al procesar los datos del recorrido"]], columns=['recorrido', 'error_msg']))
                else:
                    print('Error: Formato de datos inválido.')

                print(f'Datos del recorrido {cod_url} guardados en el csv.')
            else:
                print(f'Error al obtener los datos del recorrido: {cod_url}')
                guardar_error(cod_url, 'Error al obtener los datos del recorrido', folder_name)
        else:
            print('Error: Formato de datos inválido.')

    paraderos_df = pd.concat(paraderos_df_list, ignore_index=True)
    horarios_df = pd.concat(horarios_df_list, ignore_index=True)
    error_df = pd.concat(error_df_list, ignore_index=True)

    return paraderos_df, horarios_df, error_df


def get_current_datetime():
    return datetime.now().strftime('%Y%m%d%H%M%S')

def dfs_api_recorridos(request):

    rec_disponibles = obtener_datos('https://www.red.cl/restservice_v2/rest/getservicios/all')

    if rec_disponibles is not None:
        folder_name = get_current_datetime()
        folder_path = os.path.join('/tmp', folder_name)
        os.makedirs(folder_path, exist_ok=True)
        paraderos_df, horarios_df, error_df = guardar_datos(rec_disponibles, folder_name)
        if paraderos_df is not None and horarios_df is not None and error_df is not None:
            paraderos_df = paraderos_df[['recorrido', 'trayecto', 'name', 'comuna', 'latitud', 'longitud']]
            horarios_df = horarios_df[['recorrido', 'trayecto', 'tipoDia', 'inicio', 'fin']]
            error_df = error_df[['recorrido', 'error_msg']]

            paraderos_file_name = 'paraderos.csv'
            paraderos_df.to_csv(os.path.join(folder_path, paraderos_file_name), index=False)

            horarios_file_name = 'horarios.csv'
            horarios_df.to_csv(os.path.join(folder_path, horarios_file_name), index=False)

            error_file_name = 'errores.csv'
            error_df.to_csv(os.path.join(folder_path, error_file_name), index=False)

            print(f'Datos guardados en la carpeta {folder_name}.')
        else:
            print('No se pudieron obtener los datos de los recorridos.')
    else:
        print('No se pudieron obtener los datos de los recorridos.')

    # Subir la carpeta al bucket de Google Cloud Storage
    bucket = storage.Client().get_bucket(bucket_name)
    destination_folder_name = f'{folder_name}'
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        blob = bucket.blob(f'{destination_folder_name}/{file_name}')
        blob.upload_from_filename(file_path)

    print(f'Datos guardados en la carpeta {destination_folder_name}.')
    print(f'Carpeta {folder_name} subida al bucket {bucket_name}.')

    # Eliminar la carpeta localmente
    shutil.rmtree(folder_path)

    return 'Proceso completado con éxito.'

if __name__ == '__main__':
    dfs_api_recorridos(None)