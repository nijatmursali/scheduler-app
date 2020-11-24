import fileinput
import os
from flask.globals import session
import numpy as np
from pandas import DataFrame
import pandas as pd
import psycopg2
from flask import Flask, render_template, url_for, redirect, request, session
from datetime import datetime
import time
app = Flask(__name__)
app.secret_key = "SECRET"

# database
hostname = 'localhost'
username = 'manifestsync'
password = 'fmD384jaFdUKJSAV'
database = 'hermia'
portdb = 5433


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


if __name__ == "__main__":
    app.run(debug=True)
