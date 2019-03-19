######################################################################################################################
#
# getChicagoTrafficData.py
#
# This program makes an API call to Chicago Traffic Tracker website 
# to pull down latest traffic congestion estimates data and load the data into a MySQL 
# database table.
#
# Data is pulled from:
# https://data.cityofchicago.org/Transportation/Chicago-Traffic-Tracker-Congestion-Estimates-by-Se/n4j6-wkkf/data
#
######################################################################################################################
import json
import requests   #pip install requests
import mysql.connector as mysql
from datetime import datetime
import decimal
import time

def write_json_file(filename, dict):
    f = open(filename, 'w')
    json.dump(dict,f, indent=2)
    f.close()        

#############################################################################
#
# def generate_primary_key()
#
# This function will return a primary key for MySQL table based on a unique 
# segment_id and date_str.  date_str must be in format 'YYYY:MM:DD HH:MM:SS'
#
##############################################################################
def generate_primary_key(segment_id, date_str):

    year = date_str[2:4]   #we are assuming all years are 2000 or later
    month = date_str[5:7]
    day= date_str[8:10]
    hour = date_str[11:13]
    minute = date_str[14:16]

    primary_key = year+month+day+hour+minute+segment_id
    return int(primary_key)

##################################################################
#
# Main Program
#
# The main program is inside one infinite loop
# which runs every 5 minutes or 300 seconds
# 
# To run in packground in Linux use:
# nohup python3 getChicagoTrafficdata.py &
#
#################################################################


while True:
    start_time = datetime.now()


    ###########################################################################
    # Use API to get list of Traffic Segments
    ###########################################################################
    api_endpoint = "https://data.cityofchicago.org/resource/n4j6-wkkf.json"

    r = requests.get(api_endpoint)
    if ( r.status_code == 200):
        print("Request Status OK!")
        traffic_data_dict_list = r.json()    #this will return a list of dictionaries.  Each dictionary represents once segment traffic data
        write_json_file("traffic_data1.json",traffic_data_dict_list)   
    else:
        print ("Error Code "+str(r.status_code))

    ###########################################################################
    # Setup Database connection
    ##########################################################################
    ip = "35.192.83.240"
    username = "root"
    db_passwd = "xxx"

    conn = mysql.connect(host=ip, user=username, password=db_passwd,database="chicago")
    cursor = conn.cursor()

    table_name = "traffic_tracker"
    #table_name = "foo"

    #############################################################################
    # Loop through list of segments and INSERT into MySQL table
    #############################################################################
    records_added = 0
    records_skipped = 0
    for segment in traffic_data_dict_list:
        segment_id = segment['segmentid']
        last_updated = segment['_last_updt']

        direction = segment['_direction']
        from_street = segment['_fromst']
        to_street = segment['_tost']
        length = decimal.Decimal(segment['_length'])
        start_lon = decimal.Decimal(segment['start_lon'])
        end_lon = decimal.Decimal(segment['_lit_lon'])
        start_lat = decimal.Decimal(segment['_lif_lat'])
        end_lat = decimal.Decimal(segment['_lit_lat'])
        str_heading = segment['_strheading']
        current_speed = int(segment['_traffic'])
        street_name = segment['street']

        if '_comments' in segment.keys():
            comments = segment['_comments']
        else:
            comments = ""


        #########################################################################
        # Get Datetime Info
        # sample: 2019-03-18 00:10:31
        ##########################################################################
        last_updated_datetime_str = last_updated.replace(".0", "")   #strip off trailing '.0'

        hour = int(last_updated_datetime_str[11:13])

        last_updated_datetime = datetime.strptime(last_updated_datetime_str, '%Y-%m-%d %H:%M:%S')
        weekday_int = last_updated_datetime.weekday()
        if (weekday_int == 0):
            day_of_week = "MON"
        elif (weekday_int ==1):
            day_of_week = "TUE"
        elif (weekday_int ==2):
            day_of_week = "WED"
        elif (weekday_int ==3):
            day_of_week = "THU"
        elif (weekday_int ==4):
            day_of_week = "FRI"
        elif (weekday_int ==5):
            day_of_week = "SAT"
        elif (weekday_int ==6):
            day_of_week = "SUN"
        else:
            day_of_week = "N/A"

        #################################################################
        # Calculate Congestion level
        ################################################################
        if (current_speed < 0):
            congestion_level = "NULL"
        elif (current_speed < 10):
            congestion_level = "'HIGH'"
        elif (current_speed < 21):
            congestion_level = "'MED'"
        else:
            congestion_level = "'LOW'"
    

        ###########################################################################
        # Generate Primary Key based on Segment_ID and Last Updated Datetime
        ###########################################################################
        primary_key = generate_primary_key(segment_id, last_updated_datetime_str)
    
        #########################################################################
        # Check if Primary Key already exists.  
        # If not, then INSERT new record
        ##########################################################################
        sql = "SELECT COUNT(*) FROM " + table_name + " WHERE estimate_id = " + str(primary_key)
        try:
            cursor.execute(sql)
            result = cursor.fetchone()
            count = result[0]
            if (count == 0):
                print("result is zero: Key is Unique!"+ str(primary_key))

                #######################################################################
                # INSERT into MySQL
                #######################################################################      
                    
                sql = "INSERT INTO "+ table_name + " (" \
                    "estimate_id, " \
                    "segment_id, " \
                    "street, " \
                    "direction, " \
                    "from_street, " \
                    "to_street, " \
                    "length, " \
                    "street_heading, " \
                    "comments, " \
                    "start_long, " \
                    "start_lat, " \
                    "end_long, " \
                    "end_lat, " \
                    "current_speed, " \
                    "weekday, " \
                    "hour, " \
                    "congestion_level, " \
                    "last_updated) " \
                    "VALUES (" \
                    + str(primary_key) + ", " \
                    + str(segment_id) + ", " \
                    + "'" + street_name + "', " \
                    + "'" + direction + "', " \
                    + "'" + from_street + "', " \
                    + "'" + to_street + "', " \
                    + str(length) + ", " \
                    + "'" + str_heading + "', " \
                    + "'" + comments + "', " \
                    + str(start_lon) + ", " \
                    + str(start_lat) + ", " \
                    + str(end_lon) + ", " \
                    + str(end_lat) + ", " \
                    + str(current_speed) + ", " \
                    + "'" + day_of_week + "' ," \
                    + str(hour) + ", " \
                    + str(congestion_level) + ", " \
                    + "'" + last_updated_datetime_str + "')"
                        
                #print(sql)
                
                try:
                    cursor.execute(sql)
                    #print("INSERT success")
                    records_added = records_added+1
                except:
                    print("INSERT failed")
                    conn.rollback()
                    break
                else:
                    conn.commit()
            else:
                #print("Duplicate key " + str(primary_key))
                records_skipped = records_skipped + 1
        except:
            print ("Cannot execute sql COUNT: "+ sql)
            break

        ##################################
        # test break                     
        #################################
        num_test_records = 10
        if ((records_added + records_skipped) == num_test_records):
            print(str(num_test_records) + " records!")
            #break

    ##########################
    # Close DB Connection
    ##########################
    cursor.close()
    conn.close()

    current_time = datetime.now()


    print("started at: "+ str(start_time))
    print("finished at: "+ str(current_time))
    print("Records added: " + str(records_added))
    print("Records skipped: " + str(records_skipped))

    f = open('log.txt', 'a')
    f.write("start_time: "+str(start_time) + "\n")
    f.write("end_time: "+str(current_time) + "\n")
    f.write("Records added: " + str(records_added) + "\n")
    f.write("Records skipped: " + str(records_skipped) + "\n\n")


    f.close()
    print("sleeping...\n")
    time.sleep(300)


