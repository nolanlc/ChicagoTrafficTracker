########################################################
#
# get_traffic_tracker_data 
#
# This Cloud Function subscribes to a Pub/Sub topic.
# When triggered by Pub/Sub, it will use API to retrieve
# traffic data from Chicago Traffic Data Portal and upload
# data to Cloud Storage as a string containing list of dictionary
# objects.  Each dictionary contains a traffic segment
# For more detials goto:
#   https://data.cityofchicago.org/Transportation/Chicago-Traffic-Tracker-Congestion-Estimates-by-Se/n4j6-wkkf
#
# Add to requirements.txt:
#   google-cloud-storage
#
##########################################################

########################################################
#
# get_traffic_tracker_data 
#
# This Cloud Function subscribes to a Pub/Sub topic.
# When triggered by Pub/Sub, it will use API to retrieve
# traffic data from Chicago Traffic Data Portal and upload
# data to Cloud Storage as a string containing list of dictionary
# objects.  Each dictionary contains a traffic segment
# For more detials goto:
#   https://data.cityofchicago.org/Transportation/Chicago-Traffic-Tracker-Congestion-Estimates-by-Se/n4j6-wkkf
#
##########################################################
import base64
import json
import requests
from google.cloud import storage
from datetime import datetime

def get_traffic_data(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    
    api_endpoint = "https://data.cityofchicago.org/resource/n4j6-wkkf.json"
    r = requests.get(api_endpoint)
    if ( r.status_code == 200):
        #API will return a list of dictionaries.  Each dictonary contains a traffic segment data point
        traffic_data_dict_list = r.json()     
        msg = "Request status OK"
    else:
    	msg = "Error Code "+str(r.status_code)

    print(pubsub_message + msg)
 
    #Build data string consisting of entire dictionary list
    data = ""
    for segment in traffic_data_dict_list:
        line = str(segment) + '\n'
        data = data + line

	#Bucket
    bucket_name = "chicago_bucket"
    
    current_date_time = datetime.now()
    datetime_str = str(current_date_time)
    blob_name = "ctt" + datetime_str.replace(" ", "") + ".json"
    
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    
    print ("Upload " + blob_name + " to Cloud Storage.")
    blob.upload_from_string(data) 
   
    print("Finished executing Function.")
    
