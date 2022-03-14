import csv
import time

import psycopg2
from datetime import datetime, timezone


def getConn (csvConfigPath):
    with open(csvConfigPath, newline='') as pgfile:
        cpgfile = csv.reader(pgfile, delimiter=';', quotechar='"')
        pg_config = dict()
        for row in cpgfile:
            pg_config[row[0]] = row[1]
        conn = psycopg2.connect(
            host=pg_config["host"],
            port=pg_config["port"],
            database=pg_config["database"],
            user=pg_config["pg_user"],
            password=pg_config["pg_pwd"])
        return conn ;

def getSql(request, conn):
    cur = conn.cursor()
    cur.execute(request)
    if cur.rowcount == 0 :
            return None
    return cur.fetchall()

def executeSqlU(orderSql, params, conn):
    cur = conn.cursor()
    try:
        cur.execute(orderSql.format(*params))
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

"""
	Sql comit via psycoPG, splits the order between auto-generated ranges of keyName.
	Needs a #ANE#offsets#ANE# tag for position replacement.
	orderSql : query script
	schemaRefName : schema used for reference table
	tableRefName : reference table
	keyName : name of the field, used as primary key in reference table. Must be unique, numeric and consecutive
	supplCond : WHERE clause in reference table - don't quote "WHERE", replacement of #ANE#supplCond#ANE# tag
	maxTime : method will try to perform the query within given time, in seconds
"""
def executeSqlU_splitKey(conn, orderSql, params, tableRefName, keyName, supplCond = '0=0', maxTime = 10, schemaRefName = "annumenv"):
    sqlCounter = 'SELECT count(*),min("{0}"),max("{0}") FROM "{1}"."{2}" WHERE {3}'
    tagOffsets = '#ANE#offsets#ANE#'
    condOffsets = '"{0}" BETWEEN {1} AND {2} '
    pgCounter = getSql(sqlCounter.format(keyName,schemaRefName,tableRefName,supplCond),conn)
    number_key = pgCounter[0][0]
    min_key = pgCounter[0][1]
    max_key = pgCounter[0][2]
    current_key = min_key
    offset = 1
    count = 0
    while current_key < max_key:
        t1 = time.time()
        max_turn = current_key+offset if current_key+offset < max_key else max_key
        print("Try : "+str(max_turn-current_key) + " entries at once.")
        executeSqlU(orderSql.replace(tagOffsets,condOffsets.format(keyName, current_key, max_turn)), params, conn)
        count +=  max_turn-current_key
        current_key = max_turn + 1
        ept = time.time() - t1
        factor = maxTime/ept if ept<maxTime else maxTime/ept
        if factor > 100 : factor = 100
        if factor < 0.01 : factor = 0.01
        print("EPT : "+str(round(ept,2)) +", FACTOR : "+str(round(factor,2)))
        offset = round(offset*factor)
        print("Done : {0} entities upon {1} ({2} %)".format(count, number_key, round(count / number_key * 100, 2)))

"""
	Sql comit via psycoPG, splits the order between auto-generated ranges of LIMIT and OFFSETS (no keys given).
	Needs a #ANE#offsets#ANE# tag for position replacement.
	orderSql : query script
	schemaRefName : schema used for reference table
	tableRefName : reference table
	supplCond : WHERE clause in reference table - don't quote "WHERE"
	maxTime : method will try to perform the query within given time, in seconds
"""
def executeSqlU_splitNoKey(orderSql, params, schemaRefName, tableRefName, keyName, supplCond, maxTime, conn):
    print(1)