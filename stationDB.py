
import sqlite3
import json
from vmBroker import conn,cursor

def connectStationDatabase() : 
    conn.execute("""CREATE TABLE IF NOT EXISTS station (StationID INT PRIMARY KEY     NOT NULL,
         AntennaCnt           INT,
         Timestamp            BIGINT,
         Longitude        DOUBLE,
         Latitude         DOUBLE,
         Enable  INT,
         IP      TEXT,
         LocalIP      TEXT,
         Version    TEXT,
         ReaderIP   TEXT,
         RaderStatus    INT,
         Active  INT);""")


def insertOrReplaceStation(StationID, AntennaCnt, Timestamp, Longitude, Latitude, Enable, IP, LocalIP, Version, ReaderIP, RaderStatus, Active) :
    sqliteInsertOrReplace = """INSERT OR REPLACE INTO station (StationID,AntennaCnt,Timestamp,Longitude,Latitude,Enable, IP, LocalIP, Version, ReaderIP, RaderStatus, Active)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?);"""

    insertData = (StationID,AntennaCnt, Timestamp, Longitude, Latitude, Enable, IP, LocalIP, Version, ReaderIP, RaderStatus, Active)

    cursor.execute(sqliteInsertOrReplace, insertData)
    conn.commit()

def deleteStation(StationID):
    sql = 'DELETE FROM station WHERE StationID=?'
    cursor.execute(sql, (StationID,))
    conn.commit()

def selectAndPrintStation(StationID):
    cursor.execute("SELECT * FROM station WHERE StationID=?", (StationID,))

    rows = cursor.fetchall()

    for row in rows:
        print(row)

def selectAndPrintAllStations():
    cursor.execute("SELECT * FROM station")

    rows = cursor.fetchall()
    for row in rows:
        print(row)

def selectAndSubscribeAllStations():
    cursor.execute("SELECT * FROM station")

    rows = cursor.fetchall()
    for row in rows:
        topic_tmp = "Station_" + str(row[0])
        print(topic_tmp)
        return topic_tmp

def connectDatabase() : 
    conn.execute("""CREATE TABLE IF NOT EXISTS cow (TagID BLOB NOT NULL,
         StationID           INT,
         AntennaID           INT,
         LastSeenTimestampUTC            BIGINT,
         PeakRSSI        INT,
         ROSpecID         INT,
         TagSeenCount  INT,
         PRIMARY KEY(TagID, StationID));""")


def insertOrReplaceData( tagID, stationID, antennaID, timestamp, rssi, ros, count) :
    sqliteInsertOrReplace = """INSERT OR REPLACE INTO cow (TagID,StationID,AntennaID,LastSeenTimestampUTC,PeakRSSI,ROSpecID,TagSeenCount)
         VALUES (?,?,?,?,?,?,?);"""

    insertData = (tagID,stationID,antennaID, timestamp, rssi, ros, count)

    cursor.execute(sqliteInsertOrReplace, insertData)
    conn.commit()

def deleteData(tagID):
    sql = 'DELETE FROM cow WHERE TagID=?'
    cursor.execute(sql, (tagID,))
    conn.commit()

def selectAndPrintData(tagID):
    cursor.execute("SELECT * FROM cow WHERE TagID=?", (tagID,))

    rows = cursor.fetchall()
    for row in rows:
        print(row)

def selectAndPrintAllData():
    cursor.execute("SELECT * FROM cow")

    rows = cursor.fetchall()
    for row in rows:
        print(row)