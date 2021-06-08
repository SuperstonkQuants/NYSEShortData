# import the PostgreSQL client for Python

import psycopg2
import pandas as pd
from os import listdir
import numpy as np
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
import psycopg2.extras



from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

 
def connect():
    con = psycopg2.connect("host=localhost dbname=quant")
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    conn = con.cursor()
    return conn


def shoExchange(conn):
    conn.execute('''
        CREATE TABLE sho_exchange(
            id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            name VARCHAR NOT NULL
        )
    ''')

    exchange = ["ARCA","Amex","Chicago","NYSE","National"]
    for x in exchange:
        conn.execute("INSERT INTO sho_exchange(name) VALUES(%s)", (x,))

def exchange(conn):
    conn.execute('''
        CREATE TABLE exchange(
            id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            name VARCHAR NOT NULL
        )
    ''')

    exchange = ["NYSE","Amex","NASDAQ"]
    for x in exchange:
        conn.execute("INSERT INTO exchange(name) VALUES(%s)", (x,))

def createSymbolTable(conn):
    conn.execute('''
        CREATE TABLE symbols (
            id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            name VARCHAR(5) NOT NULL,
            fk_exchange int,
            CONSTRAINT fk_exchange
                FOREIGN KEY (fk_exchange)
                    REFERENCES exchange(id) ON DELETE RESTRICT
        );
    ''')

def createShortTable(conn):
    conn.execute('''
        CREATE TABLE shortVolume (
            id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            date date NOT NULL,
            symbol varchar(10) NOT NULL,
            fk_shoExchange int NOT NULL,
            shortVolume bigint NOT NULL,
            totalVolume bigint NOT NULL,
            market varchar(2) NOT NULL,
            shortExemptVolume bigint NOT NULL,
            CONSTRAINT fk_shoExchange
                FOREIGN KEY (fk_shoExchange)
                    REFERENCES sho_exchange(id) ON DELETE RESTRICT
        )
    ''')

def insertSymbol(conn):
    symbols = pd.read_csv('dataset.csv')
    symbols['symbol'] = symbols['symbol'].str.strip()
    symbols['exchange'] = symbols['exchange'].str.strip()

    for row in symbols.reset_index().to_dict('rows'):
        insert_query = """
            INSERT INTO symbols (name, fk_exchange) VALUES ( ('%s'), (SELECT id from exchange WHERE lower(name)=lower('%s')))""" % (row['symbol'], row['exchange'])
        conn.execute(insert_query)


conn = connect()

# print(symbols)

# shoExchange(conn)
# exchange(conn)
#createSymbolTable(conn)
#insertSymbol(conn)
def readData(conn):
    exchange = [ARCA","Amex","Chicago","Amex","NYSE","National"]

    for e in exchange:
        print(e)
        filepaths = [e + "/" + f for f in listdir("./" + e) if f.endswith('.txt')]
        df = pd.concat((pd.read_csv(f,sep='|') for f in filepaths), ignore_index=True)
        df.columns=df.columns.str.strip()
        
        df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
        df['Short Volume'] = df['Short Volume'].fillna(0).astype(np.int64)
        df['Total Volume'] = df['Total Volume'].fillna(0).astype(np.int64)
        df['Short Exempt Volume'] = df['Short Exempt Volume'].fillna(0).astype(np.int64)
        df = df.sort_values(by=['Date'])
        df.insert(2, 'Exchange', e)

        df = df[['Date', 'Symbol', 'Exchange', 'Short Volume', 'Total Volume', 'Market', 'Short Exempt Volume']]
        print(df.columns)

        insert_query = """
                    INSERT INTO shortVolume (date, symbol, fk_shoExchange, shortVolume, totalVolume, market, shortExemptVolume)
                    VALUES ( (%s), (%s), (SELECT id from sho_exchange WHERE lower(name)=lower(%s)), (%s), (%s), (%s), (%s) )"""
        result = psycopg2.extras.execute_batch(conn, insert_query, list(df.itertuples(index=False, name=None)))
        del df
        del filepaths
        del insert_query

#shoExchange(conn)
# createShortTable(conn)
readData(conn)

