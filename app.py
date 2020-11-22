from os import sep
from threading import local
from flask import Flask, render_template, url_for
from flask_apscheduler import APScheduler
from ftplib import FTP
import psycopg2
from datetime import datetime
import pandas as pd
import os
from pathlib import Path
import pathlib

# database
hostname = 'localhost'
username = 'manifestsync'
password = 'fmD384jaFdUKJSAV'
database = 'hermia'
portdb = 5433

# ftp
host = 'sync.haulersense.com'
portftp = 21212
usr = 'manifestsync'
pwd = 'fmD384jaFdUKJSAV'


app = Flask(__name__, static_url_path='/static')
scheduler = APScheduler()


@app.route('/')
def index():
    return render_template('index.html')


def printer():
    print('Hi')

# Checks if the passed date is the same as today's


def same_date(mdtm):
    today = datetime(2020, 11, 18)  # get today's date
    # today = datetime.today()
    return mdtm.date() == today.date()

# Returns list  new files added today


def newfile_check(ftp_obj):
    filenames = ftp_obj.nlst()  # list storing filesnames of files in FTP server
    new_files = list()

    for name in filenames:
        # FTP command to retrieve modifification date of file
        mdtm = ftp_obj.sendcmd('MDTM ' + name)
        # storing file mod. date in a specific format
        date = datetime.strptime(mdtm[4:], "%Y%m%d%H%M%S")
        # print(date)

        if same_date(date):
            new_files.append(name)
    return new_files

# Downloads new files and removes them from FTP server


def download_files(ftp_obj, newfiles_list):
    dir_path = os.path.dirname(os.path.abspath(__file__)) + "\\downloaded"
    local_path = dir_path
    newfiles_list = ftp_obj.nlst()
    for file in newfiles_list:
        local_fn = os.path.join(local_path, os.path.basename(file))
        local_file = open(local_fn, "wb")
        ftp_obj.retrbinary("RETR " + file, local_file.write)
        local_file.close()

        #print(file + ' downloaded')
        # ftp_obj.delete(filename)
        #print(file + ' deleted')


def after_download():
    # Simple routine to run a query on a database and print the results:
    try:
        cnn = psycopg2.connect(
            host=hostname, user=username, password=password, dbname=database, port=portdb)

        cursor = cnn.cursor()
        entries = Path('downloaded/')
        cwd = os.getcwd()

        for entry in entries.iterdir():
            pass
            # print(entry.name)

            # let say we have any file
        df = pd.read_csv(
            cwd + "\downloaded\Letture20201111193612331_2929.txt", sep='|', names=["invoice_number",
                                                                                   "quantity", "description", "value"])

        invoice_number_ftp = df['invoice_number'].tolist()

        select_all = """ SELECT EXISTS(SELECT 1 FROM bdm.available_invoices WHERE number=%s); """
        cursor.execute(
            "SELECT number FROM bdm.available_invoices")
        row = list(cursor.fetchall())
        invoice_number_db = list()
        for inum in row:
            invoice_number_db.append(inum[0])  # convert to list

        L1 = [2, 3, 4]
        L2 = [2, 5, 3]

        final = [i for i in invoice_number_ftp if i in invoice_number_db]
        print(final)

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while selecting table", error)

# Checks if new files are added today


def ftp_check():
    ftp = FTP()
    ftp.connect(host, portftp)
    ftp.login(usr, pwd)
    ftp.set_pasv(True)
    # read lines
    ftp.retrlines('LIST')
    print("FTP Connection Established at " + str(datetime.now()))
    new_files = newfile_check(ftp)

    if not new_files:
        print('No new file detected')
        ftp.quit()
        return

    # download_files(ftp, new_files) #operations on download files
    after_download()

    ftp.quit()


# scheduler.add_job(id='FTP checker', func=ftp_check, trigger='interval',
#                   seconds=1, start_date='08:00:00', end_date='20:00:00')
# scheduler.start()


ftp_check()
