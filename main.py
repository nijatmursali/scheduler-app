import enum
import fileinput
import os
from typing import Tuple, Optional
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
from ftplib import FTP

from psycopg2._psycopg import DatabaseError

from sessions import SessionState

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
            cursor.execute("""
INSERT INTO bdm.sessions (upload_date, current_state, manual_upload)
VALUES (now()::timestamptz, %s, FALSE) RETURNING id;
            """, (SessionState.NEW,))
            session_id_row = cursor.fetchone()
            if not session_id_row:
                raise DatabaseError("Cannot insert session.")
            session_id = session_id_row[0]

            # TODO: Remove the string injection and create
            #       `invoice_number_todb` as a list of tuples, each
            #       containing the session_id as well - wasteful but
            #       simplifies things a lot.
            qry = f"""INSERT INTO bdm.invoices(invoice_number, quantity, description, value, session_id) VALUES (%s, %s, %s, %s, {session_id});"""
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

# check if dir exists


@enum.unique
class SessionState(enum.IntEnum):
    NEW = 0
    OPENED = enum.auto()
    CLOSED = enum.auto()


def directory_exists(ftp, dir):
    filelist = []
    ftp.retrlines('LIST', filelist.append)
    for f in filelist:
        if f.split()[-1] == dir and f.upper().startswith('D'):
            return True
    return False


def get_last_session_for_state(state: SessionState) -> Optional[int]:
    with psycopg2.connect(
        host=hostname, user=username, password=password, dbname=database, port=portdb
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
SELECT id FROM bdm.sessions WHERE current_state = %s ORDER BY upload_date DESC
            """, (SessionState.NEW,))

            last_session_id_tuple = cursor.fetchone()
            if not last_session_id_tuple:
                return None
            last_session_id: int = last_session_id_tuple[0]

            # TODO: Move this to a stored procedure to let PostgreSQL
            #       handle integrity checks itself.
            if cursor.fetchone():
                raise DatabaseError(
                    f"More than one session is in the `{state.name}` state.")

            return last_session_id


def check_suitable_session_availability() -> Optional[Tuple[int, SessionState]]:
    session_id = get_last_session_for_state(SessionState.OPENED)
    if session_id is not None:
        # TODO: Maybe another check should be performed to make sure
        #       there are no pending sessions in the NEW state...
        return session_id, SessionState.OPENED

    session_id = get_last_session_for_state(SessionState.NEW)
    if session_id is not None:
        return session_id, SessionState.NEW

    return None


@app.route('/', methods=["POST", 'GET'])
def do_stuff():

    ### FTP ###
    host = 'sync.haulersense.com'
    portftp = 21212
    usr = 'manifestsync'
    pwd = 'fmD384jaFdUKJSAV'

    start = ""
    check = ""
    close = ""
    count = ""
    processed = ""
    total = ""

    if request.method == 'POST':
        if request.form['submit'] == 'submit_start':

            session_id, state = check_suitable_session_availability()
            if session_id is not None and state == SessionState.NEW:

                # TODO: Explicit rollback on FTP/generation failure?

                with psycopg2.connect(
                        host=hostname, user=username, password=password, dbname=database, port=portdb
                ) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("""UPDATE bdm.sessions SET current_state = %s WHERE id = %s""",
                                       (SessionState.OPENED, session_id))
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
                        ftp.retrlines('LIST')

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
                            df['number'].astype(
                                str) + df['destination_branch'].astype(str)

                        df['weight'] = df['weight'] * 1000
                        df['time_day'] = str(time.strftime("%Y-%m-%d"))
                        df['time_hour'] = str(time.strftime(
                            "%H-%M-%S-" + str(now.second*100)))

                        df = df[['invoices', 'weight', 'time_day', 'time_hour']]
                        print(df)

                        df.to_csv('out.csv', sep='|', header=False,
                                  index=False, encoding='utf-8')
                        filename = "out.csv"
                        with open(filename, "rb") as file:
                            ftp.storbinary(f"STOR {filename}", file)
                        ftp.retrlines('LIST')
                        ...

                        connection.commit()

            # Nothing else should really be done at this time, since an
            # old opened session cannot be re-opened once more, and a
            # new session gets should be already opened at this time.
            #
            # Closed sessions are not kept into account.
            # start = 'Session started'
        if request.form['submit'] == 'submit_check':
            session_id, state = check_suitable_session_availability()
            if session_id is not None and state == SessionState.OPENED:
                with psycopg2.connect(
                        host=hostname, user=username, password=password, dbname=database, port=portdb
                ) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("""
WITH session_total AS (SELECT count(*) AS total FROM bdm.sessions WHERE id = %s),
     processed_total AS (
         SELECT count(*) AS total
         FROM bdm.invoices i
                  JOIN bdm.sessions s ON s.id = i.session_id
                  JOIN bdm.processing p ON i.invoice_number = p.invoice_number
         WHERE p.process_date = now()::date
           AND s.id = %s)
SELECT p.total AS processed_count, s.total AS session_count
FROM processed_total p,
     session_total s;
                        """, (session_id, session_id))

                        totals_row = cursor.fetchone()
                        if not totals_row:
                            raise DatabaseError(
                                f"Cannot obtain totals for session {session_id}.")

                        # TODO: Put these in the template
                        processed, total = totals_row

            check = 'Session checked!'

        if request.form['submit'] == 'submit_close':
            session_id, state = check_suitable_session_availability()
            if session_id is not None and state == SessionState.OPENED:
                with connection.cursor() as cursor:
                    cursor.execute("""UPDATE bdm.sessions SET current_state = %s WHERE id = %s""",
                                   (SessionState.CLOSED, session_id))

                    # Session closed, clean things up.
                    ...

                    connection.commit()

            close = 'Session closed!'
            print("Session closed!")

    return render_template('index.html', start=start, check=check, count=count, close=close, processed=processed, total=total)


################################# END OF SESSIONS #################################


scheduler.api_enabled = True
scheduler.init_app(app)
scheduler.start()

if __name__ == "__main__":
    with app.app_context():
        app.run(debug=True)
