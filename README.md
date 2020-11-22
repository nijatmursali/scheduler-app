# Introduction

Just an app made with Python and Flask for scheduling the tasks.

## Instructions

To enable the environment

1. Windows
   `./venv/Scripts/activate`

2. Mac OS
3. Linux

To run flask:

1. `$env:FLASK_APP = "app.py"` and `flask run` and debug `$env:FLASK_ENV="development"`

## Requirements

The software should be a self-contained Linux process, running under Python 3.8, running as a Flask application that should run a background process that, every 30 minutes between 08:00 and 20:00 from Monday to Friday performs these actions:

1. Logs onto an FTP server every 30 minutes, enumerates the available files and checks whether there is a file that has been added on the same day.
2. If no new file for the day in question is available past 19:00, sends an email alert that no new file is currently available.
3. If a file is available, said file gets downloaded then deleted from the remote FTP server.
4. Once a file has been downloaded, it is then parsed, and its content validated:
   1. The file in question is CSV-like, as in, the pipe character is used as a separator for four fields: invoice number, quantity, description, value.
   2. For each row, the invoice shall be checked against a PostgreSQL database on whether it is present in the database or not.
   3. A summary email containing a list of invoices that were not found should be sent.
   4. The fields present in the file that was downloaded earlier should be inserted into a database table.

### IMPORTANT NOTE:

- The files are named like Letture<year month day hour minute seconds milliseconds>\_<number of records>.txt
- Please keep in mind that the number of records is off by one because the customer's system generating these invoices counts newlines to get that number and doesn't add a finishing newline at the end of the file.
- Records are pipe-separated, as mentioned in the specification - the first one, the invoice number _must_ be 9 characters in length and begin with 6 - records that start with anything else should be ignored. The third one contains the description, which itself can be a comma separated value of categories (which should be normalised and put in the database in a sensible way for easier retrieval), then the last one is the value, which is a normal floating point number using a dot as a decimal separator.
- The invoice number should also be validated as in, being present in the database, but not in the past.

5.  In addition to this, a simple web interface should be made available to the warehouse operators. Its interface is comprised by literally four buttons:

    1. Start session
    2. Check session status
    3. Close session
    4. Manual session upload

    **NOTE**

    1. Operators should be able to start a session if and only if no session for the day has been started yet (as in, it is a one-shot process) - there is no need to check for day overlaps and so on, processes start and end within the same day. Operators should also be able to check the current session status via the appropriate button, which basically updates a counter by checking how many records have been processed for the current session via a database table lookup and eventually by checking on the progress of the session processing task (described later). Once all records have been processed (or if a force close operation is requested), then the session should be closed, and no further processing should be possible for the day.

    2. Manual session upload performs the CSV-like file validation and database insertion but using an uploaded file as the data source. A successful upload automatically makes the session eligible to be started.

    **NOTE**

1.  Check session status basically updates a counter in the page saying something like 'Session opened for \$DATE - 123 shipments processed out of 12345 total' and so on.
1.  Manual session upload triggers the same parsing/import process as if it was fetched from the first ftp server and once the day's data is in the db, a session is created and can be started by the warehouse people.

**When a session is started, the following operations should be performed:**

- For each valid invoice, a CSV-like line shall be generated containing some information that needs to be fetched from the view mentioned earlier.
- The generated CSV file should be uploaded to another FTP server.
- A log file on a remote server (accessible via SSHFS) should be checked for a particular string pattern to see whether the upload was performed correctly and that all the generated records have been read properly.
- If the process was successfully completed, an email shall be sent containing a list of invoices that were not yet present in the database at the time of processing, and a generic summary of how many records have been generated and when.
- If the process did not complete successfully due to the downloaded file being invalid, no records were to be generated, the FTP upload operation failed, or one or more records were not correctly read, an email alert should be sent.
- The web interface should be updated to reflect the status of this operation.

What we already have available and can provide:

1. All necessary credentials for FTP, SSH, PostgreSQL, and so on.
2. Test data.
3. Ad-hoc database views for read-only access on data that does not come from the downloaded CSV files.
4. Any database table needed will be created as requested.
5. Working code for sending HTML-templated emails, for generating invoice CSV records, configuration management, FTP operations (although we'd like if the standard ftplib module would be used instead), and a Flask project that is already configured to serve static files and can access the database.
   We will be responsible for the web interface localization and for the email templates. Third party modules can be used as needed, but we would prefer to keep that number as low as possible - most of the external code needed probably already comes as a dependency of Flask anyway. For SSHFS access we already use https://github.com/PyFilesystem/pyfilesystem2 but we also have our own SSH data streaming code that can be used to capture stdout data from a remote process, if necessary.

Our codebase is already in English and we do have a testing pipeline in place - it might be nice if some unit testing is present, but if it takes too much time we can probably retrofit it later.
