import pandas as pd
import sqlite3

conn = sqlite3.connect('tariff.db')
cursor = conn.cursor()


schema = '''
            CREATE TABLE IF NOT EXISTS tariff(
              cetcode TEXT,
              description TEXT,
              su TEXT,
              id INTEGER,
              vat REAL,
              lvy INTEGER,
              exc TEXT,
              dov TEXT
            );

            CREATE TABLE IF NOT EXISTS exchange (
              code TEXT,
              name TEXT,
              rate REAL,
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS tariff_fts USING FTS5 (
              cetcode,
              description,
              id,
              vat,
              lvy
            );

            CREATE TRIGGER IF NOT EXISTS insert_tariff_fts
              AFTER insert
              ON tariff
            BEGIN
              INSERT INTO tariff_fts (cetcode, description, id, vat, lvy)
              VALUES (NEW.cetcode, NEW.description, NEW.id, NEW.vat, NEW.lvy);
            END;

            CREATE TRIGGER IF NOT EXISTS update_tariff_fts
              AFTER update
              ON tariff
            BEGIN
              UPDATE tariff_fts
              SET
                cetcode = NEW.cetcode,
                description = NEW.description,
                id = NEW.id,
                vat = NEW.vat,
                lvy = NEW.lvy
                WHERE cetcode = NEW.cetcode;
            END;

            CREATE TRIGGER IF NOT EXISTS delete_tariff_fts
              AFTER delete
              ON tariff
            BEGIN
              DELETE FROM tariff_fts
              WHERE cetcode = OLD.cetcode;
            END;
'''
cursor.executescript(schema)

tariff_file = pd.read_excel(r'tariff.xlsx')
tariff_file.rename(columns={'CET Code': 'cetcode', 'Description': 'description',
                         'SU': 'su', 'ID': 'id', 'VAT': 'vat', 'LVY': 'lvy',
                         'EXC': 'exc','DOW': 'dow'}, inplace=True)
tariff_file.to_sql('tariff',conn, if_exists='append', index=False)

exchange_file = pd.read_excel(r'exchange.xls')
exchange_file.rename(columns={'Code': 'code', 'Name': 'name', 'Rate': 'rate'}, inplace=True)
exchange_file.to_sql('exchange',conn, if_exists='append', index=False)

conn.commit();
conn.close();
