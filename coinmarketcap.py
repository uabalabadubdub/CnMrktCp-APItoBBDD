''' Sacar información de la API de CoinMarketCap; test para configurar los
bucles, sacar la información del json, crear las tablas de la BBDD e insertar
la información en la misma.

https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest
'''

import json
import sqlite3

data_retieved = '''
{
    "data": {
        "ADA": {
            "cmc_rank": 9,
            "date_added": "2017-10-01T00:00:00.000Z",
            "id": 2010,
            "last_updated": "2018-09-04T13:06:32.000Z",
            "circulating_supply": 25927070538,
            "max_supply": 45000000000,
            "name": "Cardano",
            "num_market_pairs": 46,
            "quote": {
                "USD": {
                    "last_updated": "2018-09-04T13:06:32.000Z",
                    "market_cap": 2733045711.0646396,
                    "percent_change_1h": -0.00585898,
                    "percent_change_24h": 1.48567,
                    "percent_change_7d": 2.82315,
                    "price": 0.105412823522,
                    "volume_24h": 58257568.9984096
                }
            },
            "slug": "cardano",
            "symbol": "ADA",
            "total_supply": 31112483745
        },
        "BTC": {
            "circulating_supply": 17249050,
            "cmc_rank": 1,
            "date_added": "2013-04-28T00:00:00.000Z",
            "id": 1,
            "last_updated": "2018-09-04T13:06:22.000Z",
            "max_supply": 21000000,
            "name": "Bitcoin",
            "num_market_pairs": 5969,
            "quote": {
                "USD": {
                    "last_updated": "2018-09-04T13:06:22.000Z",
                    "market_cap": 127158975673.75052,
                    "percent_change_1h": 0.498851,
                    "percent_change_24h": 1.52586,
                    "percent_change_7d": 5.20567,
                    "price": 7371.94081261,
                    "volume_24h": 4039056408.67775
                }
            },
            "slug": "bitcoin",
            "symbol": "BTC",
            "total_supply": 17249050
        },
        "ETH": {
            "circulating_supply": 101749126.5303,
            "cmc_rank": 2,
            "date_added": "2015-08-07T00:00:00.000Z",
            "id": 1027,
            "last_updated": "2018-09-04T13:06:37.000Z",
            "max_supply": null,
            "name": "Ethereum",
            "num_market_pairs": 4097,
            "quote": {
                "USD": {
                    "last_updated": "2018-09-04T13:06:37.000Z",
                    "market_cap": 29478388864.251015,
                    "percent_change_1h": 0.116127,
                    "percent_change_24h": 0.135822,
                    "percent_change_7d": 0.630485,
                    "price": 289.716382533,
                    "volume_24h": 1443724532.55325
                }
            },
            "slug": "ethereum",
            "symbol": "ETH",
            "total_supply": 101749126.5303
        },
        "MIOTA": {
            "circulating_supply": 2779530283,
            "cmc_rank": 11,
            "date_added": "2017-06-13T00:00:00.000Z",
            "id": 1720,
            "last_updated": "2018-09-04T13:06:30.000Z",
            "max_supply": 2779530283,
            "name": "IOTA",
            "num_market_pairs": 31,
            "quote": {
                "USD": {
                    "last_updated": "2018-09-04T13:06:30.000Z",
                    "market_cap": 2024659942.0403059,
                    "percent_change_1h": -0.207619,
                    "percent_change_24h": 4.25368,
                    "percent_change_7d": 2.47072,
                    "price": 0.728418018837,
                    "volume_24h": 42133255.3698387
                }
            },
            "slug": "iota",
            "symbol": "MIOTA",
            "total_supply": 2779530283
        },
        "XLM": {
            "circulating_supply": 18773717437.0417,
            "cmc_rank": 6,
            "date_added": "2014-08-05T00:00:00.000Z",
            "id": 512,
            "last_updated": "2018-09-04T13:06:23.000Z",
            "max_supply": null,
            "name": "Stellar",
            "num_market_pairs": 114,
            "quote": {
                "USD": {
                    "last_updated": "2018-09-04T13:06:23.000Z",
                    "market_cap": 4205324278.373469,
                    "percent_change_1h": 0.0415125,
                    "percent_change_24h": 0.940294,
                    "percent_change_7d": -2.10746,
                    "price": 0.224000616419,
                    "volume_24h": 46469524.3857992
                }
            },
            "slug": "stellar",
            "symbol": "XLM",
            "total_supply": 104264152998.073
        },
        "XRP": {
            "circulating_supply": 39650153121,
            "cmc_rank": 3,
            "date_added": "2013-08-04T00:00:00.000Z",
            "id": 52,
            "last_updated": "2018-09-04T13:07:05.000Z",
            "max_supply": 100000000000,
            "name": "XRP",
            "num_market_pairs": 197,
            "quote": {
                "USD": {
                    "last_updated": "2018-09-04T13:07:05.000Z",
                    "market_cap": 13421988417.454313,
                    "percent_change_1h": 0.314085,
                    "percent_change_24h": 0.198003,
                    "percent_change_7d": -2.05827,
                    "price": 0.338510380439,
                    "volume_24h": 229545412.783056
                }
            },
            "slug": "ripple",
            "symbol": "XRP",
            "total_supply": 99991852985
        }
    },
    "status": {
        "credit_count": 1,
        "elapsed": 9,
        "error_code": 0,
        "error_message": null,
        "timestamp": "2018-09-04T13:07:40.178Z"
    }
}
'''

connection = sqlite3.connect('criptoBBDD.sqlite')
cur = connection.cursor()
info = json.loads(data_retieved)

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
