# python 3.6
import random
import time
import ast
from paho.mqtt import client as mqtt_client

import sqlite3
import json
import stationDB as stdb

conn = sqlite3.connect('cloud.db', check_same_thread=False)
cursor = conn.cursor()

topic = []


broker = 'ec2-13-38-177-0.eu-west-3.compute.amazonaws.com'
port = 1883
rollCallTopic = "RollCall"
# generate client ID with pub prefix randomly
client_id = f'broker-mqtt-{random.randint(0, 1000)}'
username = 'fintes'
password = 'fintes'

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker, port)
    return client


# def publish(client):
#     msg_count = 0
#     while True:
#         time.sleep(1)
#         msg = f"messages: {msg_count}"
#         result = client.publish(topic, msg)
#         # result: [0, 1]
#         status = result[0]
#         if status == 0:
#             print(f"Send `{msg}` to topic `{topic}`")
#         else:
#             print(f"Failed to send message to topic {topic}")
#         msg_count += 1

def is_json(myjson):
  try:
    json.loads(myjson)
  except ValueError as e:
    return False
  return True

def on_message(client, userdata, msg):  # The callback for when a PUBLISH message is received from the server.
    print("Message received-> " + msg.topic + " " + str(msg.payload))  # Print a received msg
    if msg.topic == "RollCall" :
        print("devices : " + str(msg.payload))
        message = str(msg.payload)
        result = ast.literal_eval(message)
        print(is_json(result))
        if is_json(result) == True :            
            hop = json.loads(result)
            print(hop["StationID"])
            stdb.insertOrReplaceStation( hop["StationID"], hop["AntennaCnt"], hop["Timestamp"], hop["Longitude"],
                hop["Latitude"], hop["Enable"], hop["IP"], hop["Version"], hop["ReaderIP"], hop["ReaderStatus"], hop["Active"])
            topic_tmp = "Station_" + str(hop["StationID"])
            print(topic_tmp)
            client.subscribe(topic_tmp)
            if topic_tmp not in topic :
                topic.append(topic_tmp)
    elif msg.topic == "cow_test" :
        print("Cow RFID  : " + str(msg.payload))
    elif msg.topic in topic :
        message = str(msg.payload)
        result = ast.literal_eval(message)
        print(is_json(result))
        if is_json(result) == True :            
            hop = json.loads(result)
            print(hop)
            print(hop["StationID"])
            for tag in hop["Tags"]:
                print(tag["TagID"])
                print(tag["AntennaID"])
                print(tag["LastSeenTimestampUTC"])
                print(tag["PeakRSSI"])
                print(tag["ROSpecID"])
                print(tag["TagSeenCount"])
          
    else :
        print("Wrong Topic")

def publishRollCall(client):
    msg = "RollCall"
    result = client.publish(rollCallTopic, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        print(f"Send `{msg}` to topic `{rollCallTopic}`")
    else:
        print(f"Failed to send message to topic {rollCallTopic}")

def run():
    stdb.connectStationDatabase()
    stdb.connectDatabase()
    client = connect_mqtt()
    client.subscribe(rollCallTopic)
    topicTmp = stdb.selectAndSubscribeAllStations()
    print(topicTmp)
    if topicTmp != None :
        client.subscribe(topicTmp)
        topic.append(topicTmp) 
    client.loop_start()
    publishRollCall(client)
    counter = 0;
    while True:
        time.sleep(1)
        counter = counter + 1
        if counter > 60 :
            publishRollCall(client)
            counter = 0
        #publish(client)


if __name__ == '__main__':
    run()