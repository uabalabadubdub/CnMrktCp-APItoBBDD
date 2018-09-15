''' Codigo para conectarse a la API de CoinMarketCap. RESTful API.

Requiere tener un Token de autorización. El paquete básico permite 200
creditos al dia y 6000 en total por mes. Cada consulta hecha en este codigo
consume 1 crédito.

Es necesario descargar la libreria requests para Python (PIP: requests).
Dispone de código para gestionar los errores.
El código convierte los datos devueltos a diccionarios de python para posterior
adición a una BBDD.

Los parametros están configurados para devolver las criptomonedas siguientes:
    BTC,ETH,XRP,XLM,ADA,MIOTA
En USD.
'''

import requests
import json
import hiddenCoinMarketCap as CMC
import sqlite3

credits_per_month = 6000
credits_per_day = 200

# Token y URL base; guardar token personal en hiddenCoinMarketCap
api_token = CMC.headers().get('X-CMC_PRO_API_KEY')
api_url_base = 'https://pro-api.coinmarketcap.com/v1/'

# Encabezados que se añaden a la petición que se manda a la API. Incluye el Token
# y especifica el tipo de información que se esta mandando (json)
headers = {'Content-Type': 'application/json','X-CMC_PRO_API_KEY': api_token}

# Función que conecta con la API Añade los parametros de petición para la consulta
# En función de la respuesta obtenida evalua y gestiona los posibles errores
# Si la conexion es correcta (status_code = 200), devuelve un diccionario formado
# a partir de la respuesta en json dada por el servicio web
def get_cyptocurrency_quotes_latest_info():
    api_url = '{0}cryptocurrency/quotes/latest'.format(api_url_base)
    parameters = {'symbol': 'BTC,ETH,XRP,XLM,ADA,MIOTA', 'convert': 'USD'}

    response = requests.get(api_url, headers = headers, params = parameters)

    if response.status_code >= 500:
        print('[!] [{0}] Server Error'.format(response.status_code))
        cryptocurrency_quotes_info = json.loads(response.content.decode('utf-8'))
        return cryptocurrency_quotes_info
    elif response.status_code == 403:
        print('[!] [{0}] Too many requests: [{1}]'.format(response.status_code,api_url))
        cryptocurrency_quotes_info = json.loads(response.content.decode('utf-8'))
        return cryptocurrency_quotes_info
    elif response.status_code == 404:
        print('[!] [{0}] URL not found: [{1}]'.format(response.status_code,api_url))
        return None
    elif response.status_code == 403:
        print('[!] [{0}] Forbidden: [{1}]'.format(response.status_code,api_url))
        cryptocurrency_quotes_info = json.loads(response.content.decode('utf-8'))
        return cryptocurrency_quotes_info
    elif response.status_code == 401:
        print('[!] [{0}] Authentication Failed'.format(response.status_code))
        cryptocurrency_quotes_info = json.loads(response.content.decode('utf-8'))
        return cryptocurrency_quotes_info
    elif response.status_code == 400:
        print('[!] [{0}] Bad Request'.format(response.status_code))
        cryptocurrency_quotes_info = json.loads(response.content.decode('utf-8'))
        return cryptocurrency_quotes_info
    elif response.status_code >= 300:
        print('[!] [{0}] Unexpected Redirect'.format(response.status_code))
        return None
    elif response.status_code == 200:
        cryptocurrency_quotes_info = json.loads(response.content.decode('utf-8'))
        return cryptocurrency_quotes_info
    else:
        print('[?] Unexpected Error: [HTTP {0}]: Content: {1}'.format(response.status_code, response.content))
    return None

cryptocurrency_quotes_info = get_cyptocurrency_quotes_latest_info()

# Si la respuesta ha sido buena:
# intenta acceder a los valores que me interesan del diccionario y los guarda
# en una BBDD.
# Si falla esto, despliega el json recibido completo en formato legible
# Si ha surgido un error despliega los detalles del error
if cryptocurrency_quotes_info['status']['error_code'] == 0 :
    Try:
        # Abre la conexion a la BBDD
        connection = sqlite3.connect('criptoBBDD.sqlite')
        cur = connection.cursor()
        # Carga el json recibido en un diccionario de python
        info = json.loads(cryptocurrency_quotes_info)

        # Crea las tablas de la BBDD
        cur.executescript('''
            CREATE TABLE IF NOT EXISTS Criptomonedas
            (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            nombre TEXT UNIQUE
            );
            CREATE TABLE IF NOT EXISTS Tiemposextraccion
            (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
            extraccion TEXT UNIQUE
            );
            CREATE TABLE IF NOT EXISTS Monedasfiat
            (
            id INTEGER PRIMARY KEY,
            divisa TEXT UNIQUE
            );
            CREATE TABLE IF NOT EXISTS Informacion_general
            (
            cripto_id INTEGER,
            extraccion_id INTEGER,
            ultima_actualizacion TEXT,
            suministro_actual INTEGER,
            suministro_total INTEGER,
            suministro_maximo INTEGER,
            PRIMARY KEY (cripto_id, extraccion_id)
            );
            CREATE TABLE IF NOT EXISTS Informacion_precios
            (
            cripto_id INTEGER,
            extraccion_id INTEGER,
            moneda_id INTEGER,
            capitalizacion REAL,
            precio_unitario REAL,
            volume_24h REAL,
            PRIMARY KEY (cripto_id, extraccion_id, moneda_id)
            )
            ''')
        connection.commit()

        # Inserta la nueva hora de registro
        try:
            extraccion = info['status']['timestamp']
            cur.execute('''
                INSERT OR IGNORE INTO Tiemposextraccion
                (extraccion)
                VALUES (?)
                ''', (extraccion, ))
            connection.commit()
            cur.execute('SELECT id, extraccion FROM Tiemposextraccion WHERE extraccion = ? LIMIT 1', (extraccion,))
            (extraccion_id, extraccion) = cur.fetchone()
            print(extraccion_id, extraccion)
        except Exception as e:
            print('Error inserting new timestamp:', extraccion)
            print('%s \n' % e)
            exit()

        # Código para sacar la nueva id del nuevo registro temporal y
        # añadirlo a las tuplas de información

        for cripto in info['data'].keys():
            # Inserta nuevas criptomonedas si se han añadido al reporte
            try:
                cur.execute('''
                    INSERT OR IGNORE INTO Criptomonedas
                    (nombre)
                    VALUES (?)
                    ''', (cripto, ))
                connection.commit()
                cur.execute('SELECT id, nombre FROM Criptomonedas WHERE nombre = ? LIMIT 1', (cripto,))
                (cripto_id, criptomoneda) = cur.fetchone()
                print(cripto_id, criptomoneda)
            except Exception as e:
                print('Error inserting new cripto:', cripto)
                print('%s \n' % e)
                continue
            # Saca la información general del json y la almacena en una tupla para
            # añadirla posteriormente a la BBDD como una nueva linea
            for key, value in info['data'][cripto].items():
                if key == "circulating_supply":
                    suministro_actual = value
                    print ('\t', (key, suministro_actual))
                elif key == "last_updated":
                    ultima_actualizacion = value
                    print ('\t', (key, ultima_actualizacion))
                elif key =="max_supply":
                    suministro_maximo = value
                    print ('\t',(key, suministro_maximo))
                #elif key =="slug":
                #    print ('\t',key, value)
                elif key =="total_supply":
                    suministro_total = value
                    print ('\t', (key, suministro_total))
                elif key == "quote":
                    for fiat_crrcy in info['data'][cripto][key].keys():
                        # Inserta nuevas divisas si se han añadido al reporte
                        try:
                            cur.execute('''
                                INSERT OR IGNORE INTO Monedasfiat
                                (divisa) VALUES (?)
                                ''', (fiat_crrcy, ))
                            connection.commit()
                            cur.execute('SELECT id, divisa FROM Monedasfiat WHERE divisa = ? LIMIT 1', (fiat_crrcy,))
                            (moneda_id, moneda) = cur.fetchone()
                            print(moneda_id, moneda)
                        except Exception as e:
                            print('Error inserting new fiat currency:', fiat_crrcy)
                            print('%s \n' % e)
                            continue
                        for k, v in info['data'][cripto][key][fiat_crrcy].items():
                            if k == "market_cap":
                                print('\t\t',k,v)
                                tapa_mercado = v
                            elif  k == "price":
                                print('\t\t',k,v)
                                precio_unitario = v
                            elif k =="volume_24h":
                                print('\t\t',k,v)
                                volumen_24h = v
                    # Inserta valores en tabla Informacion de precios
                    # Debug print((cripto_id, extraccion_id, moneda_id, tapa_mercado, precio_unitario, volumen_24h))
                    try:
                        cur.execute('''
                        INSERT OR IGNORE INTO
                        Informacion_precios
                        (
                        cripto_id,
                        extraccion_id,
                        moneda_id,
                        capitalizacion,
                        precio_unitario,
                        volume_24h
                        )
                        VALUES
                        (
                        ?,
                        ?,
                        ?,
                        ?,
                        ?,
                        ?
                        )
                        ''',
                        (
                        cripto_id,
                        extraccion_id,
                        moneda_id,
                        tapa_mercado,
                        precio_unitario,
                        volumen_24h
                        )
                        )
                        connection.commit()
                    except Exception as e:
                        print('Error en el proceso de inserción en la tabla de precios para ', (cripto, fiat_crrcy))
                        print('%s \n' % e)
                        continue
            # Inserta valores en tabla Informacion general
            try:
                cur.execute('''INSERT OR IGNORE INTO Informacion_general
                            (
                            cripto_id,
                            extraccion_id,
                            ultima_actualizacion,
                            suministro_actual,
                            suministro_total,
                            suministro_maximo
                            )
                            VALUES
                            (
                            ?,
                            ?,
                            ?,
                            ?,
                            ?,
                            ?
                            )''',
                            (cripto_id, extraccion_id,
                            ultima_actualizacion,
                            suministro_actual,
                            suministro_total, suministro_maximo))
                connection.commit()
            except Exception as e:
                print('Error en el proceso de inserción en la tabla general para ', cripto)
                print('%s \n' % e)
                continue

        cur.close()
        connection.close()
    except Exception as e:
        print('An error ocurred when writing the info at the DDBB: ', e)
        print('=================================================================')
        print (json.dumps(cryptocurrency_quotes_info, indent=4, sort_keys=True))

elif cryptocurrency_quotes_info != None:
    for k, v in cryptocurrency_quotes_info['status'].items():
        print('{0}:{1}'.format(k, v))
