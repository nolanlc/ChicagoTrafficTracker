###########################################
#
# Hello mySQL
#
# Install mysql-connector for python:
# pip install mysql-connector-python
#
# Documentation:
#
# https://dev.mysql.com/doc/connector-python/en/connector-python-example-ddl.html
#
# Connect via CloudShell:
# gcloud sql connect mycloudsqlinstance1 --user=root --quiet
#
###########################################


import mysql.connector as mysql


print ("Hello mySQL!")

table_description = "CREATE TABLE IF NOT EXISTS traffic_tracker (" \
                    "estimate_id BIGINT NOT NULL, " \
                    "segment_id SMALLINT NOT NULL, " \
                    "street VARCHAR(64), " \
                    "direction CHAR(2) NULL," \
                    "from_street VARCHAR(64) NULL, " \
                    "to_street VARCHAR(64) NULL, " \
                    "length DECIMAL(10,9) NULL, " \
                    "street_heading CHAR(1) NULL, " \
                    "comments VARCHAR(64) NULL, " \
                    "start_long DECIMAL(10,8) NULL, " \
                    "start_lat DECIMAL(10,8) NULL, " \
                    "end_long DECIMAL(10,8) NULL, " \
                    "end_lat DECIMAL(10,8) NULL, " \
                    "current_speed TINYINT NULL," \
                    "last_updated CHAR(19) NOT NULL, " \
                    "weekday CHAR(3) NULL," \
                    "hour TINYINT NULL, " \
                    "PRIMARY KEY (estimate_id) " \
                    ") "


test_description = "CREATE TABLE IF NOT EXISTS foo (" \
                    "estimate_id BIGINT NOT NULL, " \
                    "segment_id INT NOT NULL, " \
                    "last_updated CHAR(19) NOT NULL, " \
                    "PRIMARY KEY (estimate_id) " \
                    ") "

sql = table_description
#sql = test_description

ip = "35.192.83.240"
username = "root"
db_passwd = "hxxxxxxx"
conn = mysql.connect(host=ip, user=username, password=db_passwd,database="chicago")
cursor = conn.cursor()

try:
    print("executing\n"+sql)
    cursor.execute(sql)
except:
    print ("Cannot create table")
else:
    print ("OK")
    

cursor.close()
conn.close()

