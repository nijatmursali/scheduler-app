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
                raise DatabaseError(f"More than one session is in the `{state.name}` state.")

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
    isStarted = False

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

                        # New session opened, generate entries, perform
                        # upload
                        ...

                        connection.commit()

            # Nothing else should really be done at this time, since an
            # old opened session cannot be re-opened once more, and a
            # new session gets should be already opened at this time.
            #
            # Closed sessions are not kept into account.


            # ## ftp things ##
            # ftp = FTP()
            # ftp.connect(host, portftp)
            # ftp.login(usr, pwd)
            # ftp.set_pasv(True)
            #
            # crtDir = '/session_start'
            # if directory_exists(ftp, crtDir) is True:
            #     ftp.mkd(crtDir)
            # else:
            #     ftp.cwd(crtDir)
            #
            # ftp.retrlines('LIST')  # will be in session_start folder
            #
            # now = datetime.now()
            # curr_time = now.strftime("%H:%M:%S")
            # session['isstarted'] = not isStarted
            # #print("Current Time =", curr_time)
            #
            # start = 'Session started'
            # if 'isstarted' in session:
            #     isStarted = session['isstarted']
            #     if isStarted == True:
            #         if request.form['submit'] == 'submit_start':
            #             start = 'Session already started!'
            #     else:
            #         start = 'Session successfully started!'
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
                            raise DatabaseError(f"Cannot obtain totals for session {session_id}.")

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
