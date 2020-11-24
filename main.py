from flask_apscheduler import APScheduler
from flask_mail import Mail, Message
from flask.globals import session
import numpy as np
from pandas import DataFrame
import pandas as pd
import psycopg2
from flask import Flask, render_template, url_for, redirect, request, session
from datetime import datetime
import time
import os
from ftplib import FTP

scheduler = APScheduler()

app = Flask(__name__, static_url_path='/static')
app.config['SECRET_KEY'] = 'SECRET'
app.config['DEBUG'] = True
app.config['TESTING'] = False
app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_PORT'] = 587
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USE_SSL'] = False
app.config['MAIL_USERNAME'] = '***'
app.config['MAIL_PASSWORD'] = '***'
app.config['MAIL_DEFAULT_SENDER'] = '***'
app.config['MAIL_MAX_EMAILS'] = None
app.config['MAIL_ASCII_ATTACHMENTS'] = False

mail = Mail(app)

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
    # today = datetime(2020, 11, 18)  # get today's date
    today = datetime.today()
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
    if not os.path.exists('downloaded'):
        os.makedirs('downloaded')
    else:
        return
    dir_path = os.path.dirname(os.path.abspath(__file__)) + "/downloaded/"
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

    if not os.path.exists('output'):
        os.makedirs('output')
    else:
        return
    with open("output/output.txt", 'w') as fout, fileinput.input(newfiles_list_path) as fin:
        for line in fin:
            fout.write(line)


def after_download():
    try:
        cnn = psycopg2.connect(
            host=hostname, user=username, password=password, dbname=database, port=portdb)

        cursor = cnn.cursor()
        cwd = os.getcwd()
        if not os.path.exists('output'):
            os.makedirs('output')
        else:
            return
        df = pd.read_csv(
            cwd + "/output/output.txt", sep='|', names=["invoice_number",
                                                        "quantity", "description", "value"])

        df = df.drop_duplicates()
        df = df.fillna(0)
        df['invoice_number'] = df['invoice_number'].astype(int)
        invoice_number_ftp = df.values.tolist()
        invoice_number_ftp = [i for i in invoice_number_ftp if ~np.isnan(i[0])]
        invoice_number_todb = list()
        invoice_number_todb = [i for i in invoice_number_ftp if len(
            str(int(i[0]))) == 9 and str(int(i[0]))[0:1] == '6']

        ### INSERT TO DATABASE ###
        try:
            qry = """INSERT INTO bdm.invoices(invoice_number, quantity, description, value) VALUES (%s, %s, %s, %s);"""
            cursor.executemany(qry, invoice_number_todb)
            cnn.commit()
        except (Exception, psycopg2.Error) as error:
            if(cnn):
                print("Failed to insert record into table", error)

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
                final_notpresent[i])  # SEND THIS AS EMAIL ###
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

    download_files(ftp, new_files)  # operations on download files
    after_download()


# @scheduler.task('cron', id='new_check', hour='19', day_of_week='mon-fri')
def checkNeworNot():
    ftp = FTP()
    ftp.connect(host, portftp)
    ftp.login(usr, pwd)
    ftp.set_pasv(True)
    newfile = newfile_check(ftp)
    if not newfile:
        # msg_no_new_file = Message('No new file today!', recipients=['example@gmail.com'])
        # mail.send(msg_no_new_file)
        print('NO NEW FILE! SEND EMAIL!')
        ftp.quit()
        return


ftp_check()

################################# THIS PART IS FOR SESSIONS #################################


@app.route('/', methods=["POST", 'GET'])
def do_stuff():
    start = ""
    check = ""
    close = ""
    count = ""
    isStarted = False
    if request.method == 'POST':
        if request.form['submit'] == 'submit_start':
            now = datetime.now()
            curr_time = now.strftime("%H:%M:%S")
            session['isstarted'] = not isStarted
            #print("Current Time =", curr_time)
            start = 'Session started'
            if 'isstarted' in session:
                isStarted = session['isstarted']
                if isStarted == True:
                    if request.form['submit'] == 'submit_start':
                        start = 'Session already started!'
                else:
                    start = 'Session successfully started!'
        if request.form['submit'] == 'submit_check':
            cnn = psycopg2.connect(
                host=hostname, user=username, password=password, dbname=database, port=portdb)
            try:
                cursor = cnn.cursor()
                curr_time = str(time.strftime("%Y-%m-%d"))
                #curr_time = '2020-11-20'
                print(curr_time)
                qry = """SELECT COUNT(*) FROM bdm.processing WHERE process_date=%s;"""
                cursor.execute(qry, (curr_time,))
                cnn.commit()

                count = (list(cursor.fetchall()))[0][0]
                print(count)
            except (Exception, psycopg2.Error) as error:
                if(cnn):
                    print("Failed to fetch data!", error)
            check = 'Session checked!'

        if request.form['submit'] == 'submit_close':
            close = 'Session closed!'
            print("Session closed!")

    return render_template('index.html', start=start, check=check, count=count, close=close)


################################# END OF SESSIONS #################################


scheduler.api_enabled = True
scheduler.init_app(app)
scheduler.start()

if __name__ == "__main__":
    with app.app_context():
        app.run(debug=True)
