from threading import local
from flask import Flask, render_template, url_for
from apscheduler.schedulers.background import BackgroundScheduler
from flask_apscheduler import APScheduler
from ftplib import FTP
from numpy.core.numeric import NaN
import psycopg2
from datetime import datetime
import pandas as pd
import numpy as np
import os
import fileinput
import atexit
from collections import Counter

scheduler = APScheduler()

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


def same_date(mdtm):
    today = datetime(2020, 11, 18)  # get today's date
    # today = datetime.today()
    return mdtm.date() == today.date()


def newfile_check(ftp_obj):
    filenames = ftp_obj.nlst()  # list storing filesnames of files in FTP server
    new_files = list()

    for name in filenames:
        mdtm = ftp_obj.sendcmd('MDTM ' + name)
        date = datetime.strptime(mdtm[4:], "%Y%m%d%H%M%S")

        if same_date(date):
            new_files.append(name)
    return new_files


def sendemail(ftp_obj):
    filenames = ftp_obj.nlst()  # list storing filesnames of files in FTP server
    files_to_be_sent = list()

    for name in filenames:
        mdtm = ftp_obj.sendcmd('MDTM ' + name)
        date = datetime.strptime(mdtm[4:], "%Y%m%d%H%M%S")

        if same_date(date):
            files_to_be_sent.append(name)

    print("List of not found invoices are")
    return files_to_be_sent


def download_files(ftp_obj, newfiles_list):
    dir_path = os.path.dirname(os.path.abspath(__file__)) + "\\downloaded\\"
    local_path = dir_path
    newfiles_list = ftp_obj.nlst()

    newfiles_list_path = list()
    for i in newfiles_list:
        newfiles_list_path.append(dir_path + i)

    for file in newfiles_list:
        local_fn = os.path.join(local_path, os.path.basename(file))
        local_file = open(local_fn, "wb")
        ftp_obj.retrbinary("RETR " + file, local_file.write)
        local_file.close()

        print(file + ' downloaded')
        # ftp_obj.delete(filename)
        print(file + ' deleted')

    with open("output/output.txt", 'w') as fout, fileinput.input(newfiles_list_path) as fin:
        for line in fin:
            fout.write(line)


def after_download():
    try:
        cnn = psycopg2.connect(
            host=hostname, user=username, password=password, dbname=database, port=portdb)

        cursor = cnn.cursor()
        cwd = os.getcwd()
        df = pd.read_csv(
            cwd + "\\output\\output.txt", sep='|', names=["invoice_number",
                                                          "quantity", "description", "value"])

        invoice_number_ftp = df['invoice_number'].tolist()
        for i in range(0, len(invoice_number_ftp)):
            if len(str(i)) != 9 and int(str(i)[:1] != 6):
                pass
                # print("discard!")
            else:
                #invoice_number_ftp[i] = int(invoice_number_ftp[i])
                pass

        # print(invoice_number_ftp)

        cursor.execute(
            "SELECT number FROM bdm.available_invoices")
        row = list(cursor.fetchall())
        invoice_number_db = list()
        for inum in row:
            invoice_number_db.append(inum[0])  # convert to list

        # checking for present values
        final_present = [
            i for i in invoice_number_ftp if i in invoice_number_db]
        for i in range(0, len(final_present)):
            final_present[i] = int(final_present[i])

        # checking for not present values
        final_notpresent = list(np.setdiff1d(
            invoice_number_ftp, invoice_number_db))
        final_notpresent = [x for x in final_notpresent if ~np.isnan(x)]
        for i in range(0, len(final_notpresent)):
            final_notpresent[i] = int(
                final_notpresent[i])  # SEND THIS AS EMAIL

        # print(final_notpresent)

    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while selecting table", error)


# @scheduler.task('cron', id='ftp_check', hour='8-20', minute='0, 30', day_of_week='mon-fri')
def ftp_check():
    ftp = FTP()
    ftp.connect(host, portftp)
    ftp.login(usr, pwd)
    ftp.set_pasv(True)
    ftp.retrlines('LIST')
    print("FTP Connection Established at " + str(datetime.now()))
    new_files = newfile_check(ftp)

    # if not new_files:
    #     print('No new file detected')
    #     ftp.quit()
    #     return

    # download_files(ftp, new_files)  # operations on download files
    after_download()


# @scheduler.task('cron', id='new_check', hour='19', day_of_week='mon-fri')
def checkNeworNot():
    ftp = FTP()
    ftp.connect(host, portftp)
    ftp.login(usr, pwd)
    ftp.set_pasv(True)
    newfile = newfile_check(ftp)
    if not newfile:
        print('NO NEW FILE! SEND EMAIL!')
        ftp.quit()
        return


ftp_check()

# if __name__ == '__main__':
#     app = Flask(__name__, static_url_path='/static')

#     @app.route('/')
#     def index():
#         return render_template('index.html')
#     scheduler.api_enabled = True
#     scheduler.init_app(app)
#     scheduler.start()

#     app.run()
