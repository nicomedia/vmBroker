# python 3.6
import random
import time
import ast
from paho.mqtt import client as mqtt_client
import flask
from flask import request, jsonify
from flask_cors import CORS, cross_origin

import sqlite3
import json
import stationDB as stdb
import cowDB as cow
import threading
from datetime import datetime,timedelta
import logging

logging.basicConfig(filename="logFile.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')

log = logging.getLogger()
log.setLevel(logging.INFO)

app = flask.Flask(__name__)
CORS(app, support_credentials=True)

conn = sqlite3.connect('cloud.db', check_same_thread=False)
cursor = conn.cursor()

topic = []

broker = 'ec2-13-38-177-0.eu-west-3.compute.amazonaws.com'
port = 1883
rollCallTopic = "RollCallVM"
rollCallAckTopic = "RollCallReader"
rebootTopic = "reboot"
restartTopic = "appRestart"
configTopic = "stationConfig"
# generate client ID with pub prefix randomly
client_id = f'broker-mqtt-{random.randint(0, 1000)}'
username = 'fintes'
password = 'fintes'

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            log.info("Connected to MQTT Broker!")
        else:
            log.error("Failed to connect, return code " + rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(broker, port)
    return client

def is_json(myjson):
  try:
    json.loads(myjson)
  except ValueError as e:
    return False
  return True

def on_message(client, userdata, msg):  # The callback for when a PUBLISH message is received from the server.
    log.info("Message received-> " + msg.topic + " " + str(msg.payload))  # Print a received msg
    if msg.topic == rollCallAckTopic :
        message = str(msg.payload)
        result = ast.literal_eval(message)
        if is_json(result) == True :            
            hop = json.loads(result)
            stdb.insertOrReplaceStation( hop["StationID"], hop["AntennaCnt"], hop["Timestamp"], hop["Longitude"],
                hop["Latitude"], hop["Enable"], hop["IP"], hop["LocalIP"], hop["Version"], hop["ReaderIP"], hop["ReaderStatus"], hop["Active"],
                hop["IMEI"], hop["signalQuality"], hop["sshPort"], hop["uptime"])
            topic_tmp = "Station_" + str(hop["StationID"])
            client.subscribe(topic_tmp)
            if topic_tmp not in topic :
                topic.append(topic_tmp)
    elif msg.topic == "cow_test" :
        print("Cow RFID  : " + str(msg.payload))
    elif msg.topic in topic :
        message = str(msg.payload)
        result = ast.literal_eval(message)
        if is_json(result) == True :            
            hop = json.loads(result)
            for tag in hop["Tags"]:
                cow.insertOrReplaceData(tag["TagID"], hop["StationID"], tag["AntennaID"], tag["LastSeenTimestampUTC"],
                                         tag["PeakRSSI"], tag["ROSpecID"],  tag["TagSeenCount"])
                log.info(tag["TagID"])
          
    else :
        log.error("Wrong Topic")

def publishRollCall(client):
    log.info("Publish RollCall")
    msg = "RollCall"
    result = client.publish(rollCallTopic, msg)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        log.info("Send " + msg + " to topic " + rollCallTopic)
    else:
        log.error("Failed to send message to topic " + rollCallTopic)

def publishReboot(client, stationId):
    log.info("Publish Reboot")
    jsonStation = {}
    jsonStation["StationID"] = stationId
    jsonSt = json.dumps(jsonStation)
    log.info(jsonSt)
    result = client.publish(rebootTopic, jsonSt)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        log.info("Send " + jsonSt + " to topic " + rebootTopic)
    else:
        log.error("Failed to send message to topic " + rebootTopic)

def publishRestart(client, stationId):
    log.info("Publish Restart")
    jsonStation = {}
    jsonStation["StationID"] = stationId
    jsonSt = json.dumps(jsonStation)
    log.info(jsonSt)
    result = client.publish(restartTopic, jsonSt)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        log.info("Send " + jsonSt + " to topic " + restartTopic)
    else:
        log.error("Failed to send message to topic " + restartTopic)

def publishStationConfig(client, stationId):
    log.info("Publish Station Config")
    jsonStation = {}
    jsonStation["StationID"] = stationId
    jsonStation["AntennaCnt"] = 4
    jsonStation["Longitude"] = 40
    jsonStation["Latitude"] = 40
    jsonStation["Enable"] = 1
    jsonStation["LocalIP"] = "169.254.233.0"
    jsonStation["ReaderIP"] = "169.254.194.84"
    jsonStation["sshPort"] = 2222
    jsonSt = json.dumps(jsonStation)
    log.info(jsonSt)
    result = client.publish(configTopic, jsonSt)
    # result: [0, 1]
    status = result[0]
    if status == 0:
        log.info("Send " + jsonSt + " to topic " + configTopic)
    else:
        log.error("Failed to send message to topic " + configTopic)

def run():
    stdb.connectStationDatabase()
    cow.connectDatabase()

    client = connect_mqtt()
    client.subscribe(rollCallAckTopic)

    topicTmp = stdb.selectAndSubscribeAllStations()
    if topicTmp != None :
        client.subscribe(topicTmp)
        topic.append(topicTmp) 

    client.loop_start()
    publishRollCall(client)
    counter = 0
    while True:
        time.sleep(1)
        counter = counter + 1
        if counter > 60 :
            publishRollCall(client)
            counter = 0
        

        #publish(client)

@app.route('/stations', methods=['GET'])
@cross_origin(supports_credentials=True)
def stations():
	rows = stdb.selectAndPrintAllStations()

	jsonTotal = {}
	jsonTag = []
	for row in rows:
		jsonObj = {}
		jsonObj["StationID"] = row[0]
		jsonObj["AntennaCnt"] = row[1]
		jsonObj["Timestamp"] = row[2]
		jsonObj["Longitude"] = row[3]
		jsonObj["Latitude"] = row[4]
		jsonObj["Enable"] = row[5]
		jsonObj["IP"] = row[6]
		jsonObj["LocalIP"] = row[7]
		jsonObj["Version"] = row[8]
		jsonObj["ReaderIP"] = row[9]
		jsonObj["RaderStatus"] = row[10]
		jsonObj["Active"] = row[11]
		jsonObj["IMEI"] = row[12]
		jsonObj["signalQuality"] = row[13]
		jsonObj["sshPort"] = row[14]
		jsonObj["uptime"] = row[15]
		jsonTag.append(jsonObj)

	jsonTotal["Stations"] = jsonTag
	jsonDump = json.dumps(jsonTotal)
	return jsonDump

@app.route('/cowsID', methods=['GET'])
@cross_origin(supports_credentials=True)
def cowsID():
	rows = cow.selectAndPrintAllData()

	jsonTotal = {}
	jsonTag = []
	for row in rows:
		jsonObj = {}
		jsonObj["TagID"] = row[0]
		jsonObj["StationID"] = row[1]
		jsonObj["AntennaID"] = row[2]
		jsonObj["LastSeenTimestampUTC"] = row[3]
		jsonObj["PeakRSSI"] = row[4]
		jsonObj["ROSpecID"] = row[5]
		jsonObj["TagSeenCount"] = row[6]
		jsonTag.append(jsonObj)

	jsonTotal["Tags"] = jsonTag
	jsonDump = json.dumps(jsonTotal)
	return jsonDump

@app.route('/', methods=['GET'])
@cross_origin(supports_credentials=True)
def index():
    rows = cow.selectAndPrintAllData()
    textRows = "<h1 style= \"color: #5e9ca0;\">FINTES RFID Counter</h1>" \
                "<table style=\"border-collapse: collapse; width: 100%; height: 36px;\" border=\"1\">" \
                "<tbody>" \
                "<tr style=\"height: 18px;\">" \
                "<td style=\"width: 33.3333%; height: 18px;\">COW ID</td>" \
                "<td style=\"width: 33.3333%; height: 18px;\">TimeStamp</td>" \
                "<td style=\"width: 33.3333%; height: 18px;\">Last Seen</td>" \
                "</tr>"
    # jsonRet = {}
    for row in rows:
        tag_int = row[0]
        #textRows = textRows + str(hex(tag_int)) + " - \r\n"
        tmp = row[3]/1000000 + 36000 - 3600 - 180
        converted_d1 = datetime.fromtimestamp(round(tmp))
        current_time_utc = datetime.utcnow() + timedelta(hours=3)

        textRows = textRows + "<tr style=\"height: 18px;\">" \
                                "<td style=\"width: 33.3333%; height: 18px;\">" + str(tag_int) + "</td>" \
                                "<td style=\"width: 33.3333%; height: 18px;\">" + datetime.utcfromtimestamp(tmp).strftime('%Y-%m-%d %H:%M:%S') + "</td>" \
                                "<td style=\"width: 33.3333%; height: 18px;\">" + str(current_time_utc - converted_d1) + " ago</td>" \
                                "</tr>"

    # #     print(row[3])
    # #     print(type(row[3]))
    # #     tmp = row[3]/1000000
    # #     print(datetime.utcfromtimestamp(tmp).strftime('%Y-%m-%d %H:%M:%S'))

    return textRows

from waitress import serve

if __name__ == '__main__':
	thread = threading.Thread(target=run)
	thread.daemon = True
	thread.start()
	try :
		serve(app, host="0.0.0.0", port=5000)
	except KeyboardInterrupt:
		thread.join()