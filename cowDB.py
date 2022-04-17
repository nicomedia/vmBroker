
import sqlite3
import json
import vmBroker as vm

def connectDatabase() : 
    vm.conn.execute("""CREATE TABLE IF NOT EXISTS cow (TagID BLOB     NOT NULL,
         StationID           INT,
         AntennaID           INT,
         LastSeenTimestampUTC            BIGINT,
         PeakRSSI        INT,
         ROSpecID         INT,
         TagSeenCount  INT,
         PRIMARY KEY(TagID, StationID));""")


def insertOrReplaceData(tagID, stationID, antennaID, timestamp, rssi, ros, count) :
    sqliteInsertOrReplace = """INSERT OR REPLACE INTO cow (TagID, StationID,AntennaID,LastSeenTimestampUTC,PeakRSSI,ROSpecID,TagSeenCount)
         VALUES (?,?,?,?,?,?,?);"""

    insertData = (tagID,stationID,antennaID, timestamp, rssi, ros, count)
    cursor = vm.conn.cursor()

    cursor.execute(sqliteInsertOrReplace, insertData)
    vm.conn.commit()

def deleteData(tagID):
    sql = 'DELETE FROM cow WHERE TagID=?'
    vm.cursor.execute(sql, (tagID,))
    vm.conn.commit()

def selectAndPrintData(tagID):
    vm.cursor.execute("SELECT * FROM cow WHERE TagID=?", (tagID,))

    rows = vm.cursor.fetchall()
    for row in rows:
        print(row)

def selectAndPrintAllData():
    vm.cursor.execute("SELECT * FROM cow")

    rows = vm.cursor.fetchall()
    for row in rows:
        print(row)

    return rows