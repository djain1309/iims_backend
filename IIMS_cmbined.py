# Create Time: 03202023
# Author: Wen Zhang

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly as py
import plotly.express as px
import plotly.graph_objs as go
import plotly.figure_factory as ff
import plotly.graph_objects as go
import plotly.offline
import gzip
import requests
import pygeohash as gh
import glob
import datetime as dt
import io
import base64
from pyspark.context import SparkContext
from plotly.subplots import make_subplots
from builtins import sum as python_sum
from datetime import datetime, timedelta

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import DataFrameReader, SQLContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from flask import Flask, request, render_template, send_file, jsonify, Response
from flask_cors import CORS
import os
from io import BytesIO
from zipfile import ZipFile

pd.options.display.max_columns = 1000
pd.options.display.max_rows = 1000
#pd.set_option('display.max_colwidth', None)

app = Flask(__name__, static_folder='/media/roman/storage/IIMS_CODE/5_42.95_42.9_-78.85_-78.8')
CORS(app)

# Configue Spark
sparkClassPath = os.getenv('SPARK_CLASSPATH', os.path.dirname(os.path.abspath(__file__)) + '/postgresql-42.5.0.jar')
# Populate configuration
conf = SparkConf()
conf.setAppName('application')
conf.set('spark.jars', 'file:%s' % sparkClassPath)
conf.set('spark.executor.extraClassPath', sparkClassPath)
conf.set('spark.driver.extraClassPath', sparkClassPath)
# Uncomment line below and modify ip address if you need to use cluster on different IP address
# conf.set('spark.master', 'spark://127.0.0.1:7077')
conf.set("spark.driver.memory", "20g") # set Spark driver memory, 20GB
sc = SparkContext(conf=conf)
# sqlContext = SQLContext(sc)
spark = SparkSession(sc)

spark.conf.set("spark.sql.debug.maxToStringFields", 1000)

# Read gz.parquet file by Date
def create_df(file_name, dataset_name):
    file_dir = os.path.dirname(os.path.abspath(__file__)) + '/iims-bucket/'+ dataset_name +'/' + file_name #///
    print("file_dir-----------------")
    print(file_dir)
    df = spark.read.parquet(file_dir+ '/*/*.gz.parquet') 
    return df

# Nest file
def nest_file(df):
  def read_nested_json(df):
      column_list = []
      for column_name in df.schema.names:
          # print("Outside isinstance loop: " + column_name)
          # Checking column type is ArrayType
          if isinstance(df.schema[column_name].dataType, ArrayType):
              # print("Inside isinstance loop of ArrayType: " + column_name)
              df = df.withColumn(column_name, explode(column_name).alias(column_name))
              column_list.append(column_name)

          elif isinstance(df.schema[column_name].dataType, StructType):
              # print("Inside isinstance loop of StructType: " + column_name)
              for field in df.schema[column_name].dataType.fields:
                  column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
          else:
              column_list.append(column_name)

      # Selecting columns using column_list from dataframe: df
      df = df.select(column_list)
      return df

  read_nested_json_flag = True

  while read_nested_json_flag:
    # print("Reading Nested JSON File ... ")
    df = read_nested_json(df)
    read_nested_json_flag = False

    for column_name in df.schema.names:
      if isinstance(df.schema[column_name].dataType, ArrayType):
        read_nested_json_flag = True
      elif isinstance(df.schema[column_name].dataType, StructType):
        read_nested_json_flag = True
  return df

def create_data_DataFrame_from_csv_AETID(file_path):
    data = pd.concat(map(pd.read_csv, glob.glob(file_path + '/*.csv')))
    data_df = pd.DataFrame(data)
    return data_df

def data_clearning(df, dataset):
    if dataset=='Driving_Events':
        df['capturedDateTime'] = df['capturedDateTime'].apply(lambda x: x.split('.')[0])
        df['capturedDateTime'] = pd.to_datetime(df['capturedDateTime'])
        
        df['Date'] = pd.to_datetime(df['capturedDateTime']).dt.date
        df['Time'] = pd.to_datetime(df['capturedDateTime']).dt.time

        df.rename(columns = {'location_latitude':'latitude', 'location_longitude':'longitude', 
        'location_geohash':'geohash', 'location_postalCode':'postalCode', 
        'location_regionCode':'regionCode', 'location_countryCode':'countryCode', 'metrics_speed':'speed',
        'metrics_heading':'direction', 'event_eventType': 'eventType', 'event_eventMetadata_journeyEventType':'journeyEventType', 'event_eventMetadata_accelerationType': 'accelerationType' }, inplace = True)
        
        df = df.dropna(subset = ['accelerationType'])
        df['accelerationType'] = df['accelerationType'].astype('category')
        return df
    
    elif dataset == 'Vehicle_Movements':
        df['capturedTimestamp'] = df['capturedTimestamp'].apply(lambda x: x.split('.')[0])
        df['capturedTimestamp'] = pd.to_datetime(df['capturedTimestamp'])
        
        df['Date'] = pd.to_datetime(df['capturedTimestamp']).dt.date
        df['Time'] = pd.to_datetime(df['capturedTimestamp']).dt.time

        df.rename(columns = {'location_latitude':'latitude', 'location_longitude':'longitude', 
        'location_geohash':'geohash', 'location_postalCode':'postalCode', 
        'location_regionCode':'regionCode', 'location_countryCode':'countryCode', 'metrics_speed':'speed',
        'metrics_heading':'direction'}, inplace = True)
        return df

# Calculate the center of the map
def calculate_center(points):
    center_x = python_sum(point[0] for point in points) / len(points)
    center_y = python_sum(point[1] for point in points) / len(points)
    return center_x, center_y


# Map API token
token = 'pk.eyJ1Ijoid3poYW5nNTkiLCJhIjoiY2wzcDl0dmVoMHc0YzNkbTl5bnNlNWZqMSJ9.LCvs7uSWtm5en5IYPEZUkw'

generate_map_Vehicle_Movements_dic = {}

# Generating Driving_Events Map
def generate_map_Driving_Events(df, map_name, combined_range, long, lati, interval, miles):
    fig = px.scatter_mapbox(df, lon='longitude', lat = 'latitude',
                            color = 'accelerationType', title = map_name, 
                            hover_data =['journeyId', 'eventType', 'Time', 'Speed'],
                            color_discrete_sequence=['green', 'yellow', 'red'],
                            color_discrete_map={'HARD_BRAKE': 'red', 0: 'yellow', 'HARD_ACCELERATION': 'green'},
                            range_color = [0,80],
                            )
    fig.update_layout(
        hovermode='closest',
        width=1390, height=850,
        mapbox=dict(
            accesstoken=token,
            bearing=0,
            center=go.layout.mapbox.Center(
                lat = lati,
                lon = long,
            ),
            pitch=0,
            zoom=12,
        )
    )
    fig.update_layout(
        margin=dict(l=30, r=30, t=100, b=30),  
    )
    folder_name = os.path.dirname(os.path.abspath(__file__))+ '/' + str(interval) + '_' + combined_range

    if folder_name in generate_map_Vehicle_Movements_dic:
        generate_map_Vehicle_Movements_dic[folder_name] = generate_map_Vehicle_Movements_dic[folder_name] + '@' + map_name
    else:
        generate_map_Vehicle_Movements_dic[folder_name] = map_name

    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    fig.write_html(folder_name + '/' + map_name + ".html")

# Generating Vehicle_Movements Map
def generate_map_Vehicle_Movements(df, map_name, combined_range, long, lati, interval, miles):

    colorscale = [[0.0, 'red'],[0.5, 'yellow'], [1.0, 'green']]
    colors = ['red', 'yellow','green']
    fig = px.scatter_mapbox(df, lon='longitude', lat = 'latitude',
                            color = 'Speed', title = map_name, 
                            hover_data =['journeyId','Time', 'Speed'],
                            size_max= 3,
                            size='Size',
                            color_continuous_scale = colorscale,
                            range_color = [0,80],
                            )
    fig.update_layout(
        hovermode='closest',
        width=1390, height=850,
        mapbox=dict(
            accesstoken=token,
            bearing=0,
            center=go.layout.mapbox.Center(
                lat = lati,
                lon = long,
            ),
            pitch=0,
            zoom=12,
        )
    )
    fig.update_layout(
        margin=dict(l=30, r=30, t=100, b=30),  
    ) 
    #folder_name = os.path.dirname(os.path.abspath(__file__)) + '/' + str(interval) + '_' + combined_range 
    folder_name = os.path.dirname(os.path.abspath(__file__)) + '/' + str(interval) + '_' + combined_range 
    print("folder_name: "+folder_name)

    if folder_name in generate_map_Vehicle_Movements_dic:
        generate_map_Vehicle_Movements_dic[folder_name] = generate_map_Vehicle_Movements_dic[folder_name] + '@' + map_name
    else:
        generate_map_Vehicle_Movements_dic[folder_name] = map_name
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    print("folder_name map_name-----------------")
    print(folder_name + '/' + map_name)
    fig.write_html(folder_name + '/' + map_name + ".html")
    #send...



def file_to_CSV(file_name, dataset, latitude_down, latitude_up, longitude_left, longitude_right, date, file_name_output):
    print("INSIDE FILE TO CSV")
    df = create_df(file_name, dataset)
    df = nest_file(df)
    # get selected_area
    selected_area = df.filter((df.location_latitude >= latitude_down) & (df.location_latitude <= latitude_up) & 
                            (df.location_longitude >= longitude_left) & (df.location_longitude <= longitude_right))
    df_single_partition = selected_area.coalesce(1)
    print("INSIDE FILE TO CSV ALMOST OUT")
    df_single_partition.write.option("header","true").csv(os.path.dirname(os.path.abspath(__file__)) + "/CSV/" + date + "/" + file_name_output) # generate new csv file and overwrite the previous one. 

@app.route('/process-data', methods=['POST'])
def main():
    latitude_up = float(request.json['latitude'])
    latitude_down = float(request.json['latitudeDown'])
    longitude_left = float(request.json['longitudeLeft']) # need to change the name of "longitude" and "longitudeLeft" in front end !!!
    longitude_right = float(request.json['longitude'])
    dataset = request.json['datasetName']
    file_name = request.json['fileName']
    interval = int(request.json['interval'])       # Time interval
    start_hour = int(request.json['startHour'])     # start time (hour)
    start_min = int(request.json['startMin'])      # start time (minute)
    end_hour = int(request.json['endHour'])       # end time (hour)
    end_min = int(request.json['endMin'])        # end time (minute)   
    
    # incident: 42.9039, -78.8456
    incident_latitude = 42.9039
    incident_longitude = -78.8456
    miles = 5
    # latitude_up = incident_latitude + 0.0145 * miles
    # latitude_down = incident_latitude - 0.0145 * miles
    # longitude_left = incident_longitude - 0.0145 * miles
    # longitude_right = incident_longitude + 0.0145 * miles

    # latitude_up = 42.93850592054101
    # latitude_down = 42.86472437321413
    # longitude_left = -78.87244222450173
    # longitude_right = -78.72064457179688

    # latitude_up = 42.90794558273012
    # latitude_down = 42.873542451547365
    # longitude_left = -78.88474811367854
    # longitude_right = -78.83797717817932

    combined_range = str(latitude_up)+ "_" + str(latitude_down) + "_" + str(longitude_left) + "_" + str(longitude_right)
    print(combined_range)

    # Parameters for generating the map
    # interval = 10      # Time interval
    # start_hour = 20     # start time (hour)
    # start_min = 0      # start time (minute)
    # end_hour = 20       # end time (hour)
    # end_min = 30        # end time (minute)

    # dataset = 'Vehicle_Movements' #request.json['dataset'] #'Driving_Events' #'Driving_Events', 'Vehicle_Movements'
    # file_name = 'dt=2023-03-01' #request.json['file_name'] #'dt=2022-09-29' # date of the file


    date_st = dt.datetime.strptime(file_name.split('=')[1], '%Y-%m-%d')
    date2 = date_st + timedelta(days=1)
    file_name2 = 'dt=' + date2.strftime('%Y-%m-%d')
    file_name2_date = date2.strftime('%Y-%m-%d').split('-')[1]+date2.strftime('%Y-%m-%d').split('-')[2]+date2.strftime('%Y-%m-%d').split('-')[0] # 09292022

    file_name_temp = file_name.split('=')[1]
    date = file_name_temp.split('-')[1]+file_name_temp.split('-')[2]+file_name_temp.split('-')[0] # 09292022
    file_name_output = date + '_selected_area_' + combined_range + '_' + dataset
    file_path = os.path.dirname(os.path.abspath(__file__)) + '/CSV/' + date + '/' + file_name_output

    file_name_output2 = file_name_output.replace(date, file_name2_date)
    print("file_name_output2 => ", file_name_output2)
    file_path2 = file_path.replace(date, file_name2_date)
    date2 = file_name2_date
    

    if os.path.exists(file_path + "_whole.csv"):
        # df_date = pd.read_csv(file_path + "_whole.csv")
        # df_date = pd.read_csv(file_path + "_whole.csv", parse_dates=['Time'])
        df_date = pd.read_csv(file_path + "_whole.csv")
        df_date['Time'] = pd.to_datetime(df_date['Time'], format='%H:%M:%S').dt.time
    else:
        if not os.path.exists(file_path):
            file_to_CSV(file_name, dataset, latitude_down, latitude_up, longitude_left, longitude_right, date, file_name_output)
        if not os.path.exists(file_path2):
            file_to_CSV(file_name2, dataset, latitude_down, latitude_up, longitude_left, longitude_right, date2, file_name_output2)
        # Creating dataframe and clearning data
        df_1 = create_data_DataFrame_from_csv_AETID(file_path)
        print("df_1.size", df_1.size)
        df_2 = create_data_DataFrame_from_csv_AETID(file_path2)
        print("df_2.size", df_2.size)
        df = pd.concat([df_1, df_2], axis=0)
        df = data_clearning(df, dataset)
        df["Date"] = df["Date"].apply(lambda x: str(x))
        mask = (df['Date'] == file_name_temp)
        df_date = df.loc[mask] 
        df_date.to_csv(file_path + "_whole.csv")
    # df_date.sort_values(by=['Time'], ascending=True, inplace=True)
    # Generating maps
    start_time = dt.time(start_hour, start_min)
    end_time = dt.time(end_hour, end_min)

    long = ''
    while start_time < end_time:
        # print(f"start_time:{start_time.strftime('%H:%M')}")
        compare_datetime = dt.datetime.combine(dt.date.today(), start_time)
        new_time = (compare_datetime + dt.timedelta(minutes=interval)).time()
        #print(f"new_time:{new_time.strftime('%H:%M')}")

        mask = (df_date['Time']>=start_time) & (df_date['Time']< new_time)
        df_date_time = df_date.loc[mask] 
        df_date_time["Speed"] = df_date_time["speed"].apply(lambda x: x*0.62137)
        df_date_time["Size"] = 3

        if long == '': #only calculate once

            points = df_date_time[['longitude', 'latitude']].values.tolist()

            if (len(points) > 0):
                long, lati = calculate_center(points)
            else:
                long = -78.85
                lati = 42.93
            long, lati = calculate_center(points)

        # generated_map = NullType

        if dataset == 'Driving_Events':
            #Generating Driving_Events map
            generate_map_Driving_Events(df_date_time, file_name_temp+'_' + start_time.strftime('%H:%M:%S') + '-' + new_time.strftime('%H:%M:%S') + '_Wejo_' + dataset + '_Data', combined_range, long, lati, interval, miles)
        elif dataset == 'Vehicle_Movements':
            #Generating Vehicle_Movements map
            generate_map_Vehicle_Movements(df_date_time, file_name_temp+'_' + start_time.strftime('%H:%M:%S') + '-' + new_time.strftime('%H:%M:%S') + '_Wejo_' + dataset + '_Data', combined_range, long, lati, interval, miles)
        
        start_time = new_time

        # fixed X to 00:00 
        if start_time == dt.time(0, 0): #  subtract 1 minute from the start_time(new_time) if it is equal to 00:00, making it 23:59.
            start_datetime = dt.datetime.combine(dt.date.today(), start_time) - dt.timedelta(minutes=1)
            start_time = start_datetime.time()


# 42.95   42.9    -78.85   -78.8
    # WORKING CODE
    html_file_path = ""
    for folder in generate_map_Vehicle_Movements_dic.keys():
        print("--------FOLDER------------", folder)
        folder_name = folder #//////
        map_names = generate_map_Vehicle_Movements_dic[folder_name]
        map_names_list = map_names.split("@")
        html_file_path = ""
        for ele in map_names_list:
            map_name = ele #'2023-03-01_20:20:00-20:30:00_Wejo_Vehicle_Movements_Data'
            html_file_path = html_file_path + os.path.join(folder_name, map_name + '.html') + "@"
    
    generate_map_Vehicle_Movements_dic.clear()
    print("html_file_path=====>>> ", html_file_path)
    return html_file_path

@app.route('/get_html_file/', methods=['GET'])
def get_html_file():
    url = request.args.get('url')

    try:
        return send_file(url, mimetype='text/html')
    except:
        return 'Error: File not found'
    # return url


if __name__ == "__main__":
#    main()
    app.run()