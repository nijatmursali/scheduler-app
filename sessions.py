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

from psycopg2._psycopg import DatabaseError

app = Flask(__name__)
app.secret_key = "SECRET"

# database
hostname = 'localhost'
username = 'manifestsync'
password = 'fmD384jaFdUKJSAV'
database = 'hermia'
portdb = 5433


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

                        df.to_csv('output/out.csv', sep='|', header=False,
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

    return render_template('index.html', start=start, check=check, count=count, close=close)


if __name__ == "__main__":
    app.run(debug=True)
