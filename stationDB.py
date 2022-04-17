
import sqlite3
import json
import vmBroker as vm

def connectStationDatabase() : 
    vm.conn.execute("""CREATE TABLE IF NOT EXISTS station (StationID INT PRIMARY KEY     NOT NULL,
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
         Active  INT,
         IMEI BLOB,
         signalQuality DOUBLE,
         sshPort INT,
         uptime DOUBLE);""")


def insertOrReplaceStation(StationID, AntennaCnt, Timestamp, Longitude, Latitude, Enable, IP, LocalIP, Version, ReaderIP, RaderStatus, Active, IMEI, signalQuality, sshPort, uptime) :
    sqliteInsertOrReplace = """INSERT OR REPLACE INTO station (StationID,AntennaCnt,Timestamp,Longitude,Latitude,Enable, IP, LocalIP, Version, ReaderIP, RaderStatus, Active, IMEI, signalQuality, sshPort, uptime)
         VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"""

    insertData = (StationID,AntennaCnt, Timestamp, Longitude, Latitude, Enable, IP, LocalIP, Version, ReaderIP, RaderStatus, Active, IMEI, signalQuality, sshPort, uptime)

    vm.cursor.execute(sqliteInsertOrReplace, insertData)
    vm.conn.commit()

def deleteStation(StationID):
    sql = 'DELETE FROM station WHERE StationID=?'
    vm.cursor.execute(sql, (StationID,))
    vm.conn.commit()

def selectAndPrintStation(StationID):
    vm.cursor.execute("SELECT * FROM station WHERE StationID=?", (StationID,))

    rows = vm.cursor.fetchall()

    for row in rows:
        print(row)

def selectAndPrintAllStations():
    vm.cursor.execute("SELECT * FROM station")

    rows = vm.cursor.fetchall()
    for row in rows:
        print(row)
    return rows

def selectAndSubscribeAllStations():
    vm.cursor.execute("SELECT * FROM station")

    rows = vm.cursor.fetchall()
    for row in rows:
        topic_tmp = "Station_" + str(row[0])
        return topic_tmp
