from __future__ import print_function
import psycopg2

hostname = 'localhost'
username = 'manifestsync'
password = 'fmD384jaFdUKJSAV'
database = 'hermia'
port = 5433

# Simple routine to run a query on a database and print the results:

try:
    cnn = psycopg2.connect(
        host=hostname, user=username, password=password, dbname=database, port=port)
except:
    print("YOU SHALL NOT PASS!!!")

cur = cnn.cursor()

# try:
#     sql = """
#     CREATE TABLE bdm.invoices
#     (
#         invoice_number integer NOT NULL
#     )
#     WITH (
#         OIDS = FALSE
#     )
#     TABLESPACE pg_default;

#     ALTER TABLE bdm.invoices
#         OWNER to manifestsync;
#     """
#     cur.execute(sql)
#     print("Table created successfully!")

#     cur.execute(""" INSERT INTO bdm.available_invoices (number, destination_branch, date, partial, weight) VALUES (600591827, 'LT', '2020-11-20', True, 0.3); """)
#     cnn.commit()

# except (Exception, psycopg2.DatabaseError) as error:
#     print("Error while creating PostgreSQL table", error)


try:
    sql = """
    INSERT INTO bdm.invoices(
	invoice_number)
	VALUES (1);
    """
    cur.execute(sql)
    cnn.commit()
    print("Table updated successfully!")
except (Exception, psycopg2.DatabaseError) as error:
    print("Error while updating PostgreSQL table", error)

# try:
#     cur.execute("""SELECT "number", destination_branch, date, partial, weight
# 	FROM bdm.available_invoices;""")
# except:
#     print("NOPE! Don't even try that")

# rows = cur.fetchall()
# for row in rows:
#     print("   ", row)
# cnn.close()
