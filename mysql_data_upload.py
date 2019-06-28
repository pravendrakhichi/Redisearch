import MySQLdb
import csv

dbs = MySQLdb.connect(host='127.0.0.1', user='root', password='', port=3306)
cursor = dbs.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS Checkout CHARACTER SET utf8 COLLATE utf8_general_ci")#,use_unicode=True,charset='utf8')
cursor.close()


mydb = MySQLdb.connect(host='127.0.0.1', user='root', password='', port=3306,db ='Checkout')

mydb.set_character_set('utf8')

mycursor = mydb.cursor()
mycursor.execute('SET NAMES utf8;')
mycursor.execute('SET CHARACTER SET utf8;')
mycursor.execute('SET character_set_connection=utf8;')
mycursor.execute("CREATE TABLE IF NOT EXISTS customers(id INT AUTO_INCREMENT PRIMARY KEY,UsageClass VARCHAR(255),CheckoutType VARCHAR(255),MaterialType VARCHAR(255),CheckoutYear YEAR,CheckoutMonth INT,Checkouts INT,Title VARCHAR(1500),Creator VARCHAR(255),Subjects VARCHAR(1024),Publisher VARCHAR(255),PublicationYear VARCHAR(255))")


csv_data = csv.reader(open('12.csv', 'r'))
c = 0
sql = "INSERT INTO customers(UsageClass,CheckoutType,MaterialType,CheckoutYear,CheckoutMonth,Checkouts,Title,Creator,Subjects,Publisher,PublicationYear) VALUES(%s, %s,%s, %s,%s, %s,%s, %s,%s, %s,%s)"

with open("12.csv", encoding="utf8") as csvfile:
    csvreader = csv.reader(csvfile, delimiter=",")

    for row in csvreader:
        if c==0:
            c=1
            continue
        val = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10])
        mycursor.execute(sql, val)
        c += 1
        '''if (c == 500):
            break'''
        print(c)
        mydb.commit()
#mycursor.execute("SHOW TABLES")

# for x in mycursor:
#    print(x)

mycursor.close()
