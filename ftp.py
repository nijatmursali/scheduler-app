import enum
import fileinput
import os
from typing import Tuple, Optional

from flask.globals import session
import numpy as np
from pandas import DataFrame
import pandas as pd
import psycopg2
from flask import Flask, render_template, url_for, redirect, request, session
from datetime import datetime
import time
from ftplib import FTP

# database
hostname = 'localhost'
username = 'manifestsync'
password = 'fmD384jaFdUKJSAV'
database = 'hermia'
portdb = 5433

### FTP ###
host = 'sync.haulersense.com'
portftp = 21212
usr = 'manifestsync'
pwd = 'fmD384jaFdUKJSAV'


def directory_exists(ftp, dir):
    filelist = []
    ftp.retrlines('LIST', filelist.append)
    for f in filelist:
        if f.split()[-1] == dir and f.upper().startswith('D'):
            return True
    return False


with psycopg2.connect(
    host=hostname, user=username, password=password, dbname=database, port=portdb
) as connection:
    with connection.cursor() as cursor:
        ftp = FTP()
        ftp.connect(host, portftp)
        ftp.login(usr, pwd)
        ftp.set_pasv(True)

        crtDir = '/session_start'
        if directory_exists(ftp, crtDir) is True:
            ftp.mkd(crtDir)
        else:
            ftp.cwd(crtDir)

            # will be in session_start folder
        now = datetime.now()
        #curr_time = str(time.strftime("%Y-%m-%d"))
        curr_time = '2020-11-20'
        # LT<invoice number><invoice destination>|<weight * 1000>|0|0|0||GN|<current year-month-day>|<current hour-minute-seconds-centiseconds>|0|0|0|0|0||||||0|1|0|
        qry = """SELECT "number", destination_branch, date, partial, weight FROM bdm.available_invoices WHERE date=%s;"""
        cursor.execute(qry, (curr_time,))
        connection.commit()

        toFTP = (list(cursor.fetchall()))
        list_of_lists = [list(elem) for elem in toFTP]
        # print(list_of_lists)
        df = pd.DataFrame(list_of_lists, columns=[
                          "number", "destination_branch", "date", "partial", "weight"])
        df['invoices'] = 'LT' + \
            df['number'].astype(str) + df['destination_branch'].astype(str)

        df['weight'] = df['weight'] * 1000
        df['time_day'] = str(time.strftime("%Y-%m-%d"))
        df['time_hour'] = str(time.strftime("%H-%M-%S-" + str(now.second*100)))
        df = df[['invoices', 'weight', 'time_day', 'time_hour']]
        print(df)

        df.to_csv('out.csv', sep='|', header=False,
                  index=False, encoding='utf-8')

        filename = "out.csv"
        with open(filename, "rb") as file:
            ftp.storbinary(f"STOR {filename}", file)
        ftp.retrlines('LIST')
