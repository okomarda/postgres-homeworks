"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2

file = 'north_data/employees_data.csv'
file_cust = 'north_data/customers_data.csv'
file_orders = 'north_data/orders_data.csv'

all = []
all_cust = []
all_orders = []

with open (file, newline='') as csvfile :
     reader = csv.DictReader (csvfile)
     for row in reader:
         row_list = list(row.values())
         all.append(row_list)

with open (file_cust, newline='') as cust_file :
    reader = csv.DictReader (cust_file)
    for row in reader :
        row_list_cust = list (row.values ( ))
        all_cust.append (row_list_cust)

with open (file_orders, newline='') as orders_file :
    reader = csv.DictReader (orders_file)
    for row in reader :
        row_list_orders = list (row.values ( ))
        all_orders.append (row_list_orders)

conn = psycopg2.connect(
    host="localhost",
    dbname="north",
    user="postgres",
    password="Bension1904++"
)

cur = conn.cursor()
cur.executemany("INSERT INTO employees VALUES(%s,%s,%s,%s,%s,%s)", all)
cur.executemany("INSERT INTO customers VALUES(%s,%s,%s)", all_cust)
cur.executemany("INSERT INTO orders VALUES(%s,%s,%s,%s,%s)", all_orders)
conn.commit()

cur.close()
conn.close()



#print(all)