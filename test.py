import psycopg2
from datetime import datetime
import pandas as pd
from pandas import DataFrame
import numpy as np
import os
import fileinput


# database
hostname = 'localhost'
username = 'manifestsync'
password = 'fmD384jaFdUKJSAV'
database = 'hermia'
portdb = 5433

cnn = psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database, port=portdb)

cursor = cnn.cursor()
cwd = os.getcwd()
if not os.path.exists('output'):
    os.makedirs('output')
else:
    pass
df = pd.read_csv(cwd + "/output/output.txt", sep='|', names=["invoice_number",
                                                             "quantity", "description", "value"])
df = df.drop_duplicates()
df = df.fillna(0)
df['invoice_number'] = df['invoice_number'].astype(int)
invoice_number_ftp = df.values.tolist()
invoice_number_ftp = [i for i in invoice_number_ftp if ~np.isnan(i[0])]
invoice_number_todb = list()
invoice_number_todb = [i for i in invoice_number_ftp if len(
    str(int(i[0]))) == 9 and str(int(i[0]))[0:1] == '6']

try:
    qry = """INSERT INTO bdm.invoices(invoice_number, quantity, description, value) VALUES (%s, %s, %s, %s);"""
    cursor.executemany(qry, invoice_number_todb)
    cnn.commit()
except (Exception, psycopg2.Error) as error:
    if(cnn):
        print("Failed to insert record into mobile table", error)
