# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
from src.RDBDataTable import RDBDataTable
import logging
import os
import json
import pymysql

#CSV

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")

def t_csv_load():

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    print("Created table = " + str(csv_tbl))

def t_csv_match():
    row = {"cool":"yes","db":"no"}
    t = {"cool":"yes"}
    result = CSVDataTable.matches_template(row,t)
    print(result)

def t_csv_matchall():
    tmp = {"playerID":"willite01","nameLast":"Williams","nameFirst":"Ted"}
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    result = csv_tbl.find_by_template(tmp)
    print(json.dumps(result, indent=2))

    result = csv_tbl.find_by_template(tmp, ["nameLast","nameFirst","finalGame"])
    print(json.dumps(result, indent=2))

def t_csv_key2tmp():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID", "nameLast", "nameFirst"])
    k = csv_tbl.key_to_template(["willite01", "Williams", "Ted"])
    print(json.dumps(k, indent=2))

def t_csv_primkey():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID", "nameLast", "nameFirst"])
    k = csv_tbl.find_by_primary_key(["willite01","Williams","Ted"])
    print(json.dumps(k, indent=2))

def t_csv_delbytmp():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)
    tmp = {"playerID": "willite01", "nameLast": "Williams", "nameFirst": "Ted"}
    count = csv_tbl.delete_by_template(tmp)
    print(count)

def t_csv_updbytmp():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)
    tmp = {"playerID": "willite01", "nameLast": "Williams", "nameFirst": "Ted"}
    new_values = {"nameLast": "Zong", "nameFirst": "Amanda"}
    count = csv_tbl.update_by_template(tmp,new_values)
    print(count)
    k = csv_tbl.find_by_template({"playerID":"willite01","nameLast":"Zong","nameFirst":"Amanda"})
    print(json.dumps(k, indent=2))

def t_csv_updbykey():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID", "nameLast", "nameFirst"])
    k = ["willite01", "Williams", "Ted"]
    new_values = {"nameLast": "Zong", "nameFirst": "Amanda"}
    count = csv_tbl.update_by_key(k, new_values)
    print(count)

def t_csv_insert():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID", "nameLast", "nameFirst"])
    new_values = {"playerID":"amz2136","nameLast": "Zong", "nameFirst": "Amanda"}
    csv_tbl.insert(new_values)

    result = csv_tbl.find_by_template(new_values)
    print(json.dumps(result, indent=2))

#RDB

def t_rdb_load():
    connect_info = {
        "host": "localhost",
        "user": "root",
        "password": "Databases",
        "db": "w4111",
        "charset": "utf8mb4"
    }

    rdb_tbl = RDBDataTable("people",connect_info,None)

def t_rdb_primtmp():
    connect_info = {
        "host": "localhost",
        "user": "root",
        "password": "Databases",
        "db": "w4111",
        "charset": "utf8mb4"
    }

    rdb_tbl = RDBDataTable("people", connect_info, None)

    tmp = {
        "birthCity" : "San Diego",
        "nameLast" : "Williams"
    }
    fields = {
        "playerID","nameLast","nameFirst"
    }

    print(json.dumps(rdb_tbl.find_by_template(tmp, fields), indent=2))

# sql = "select playerID, nameLast, nameFirst, birthCity, birthState, birthYear from People where birthCity='San Diego' " + \
# "and nameLast='Williams'"
# cur = default_cnx.cursor()
# res = cur.execute(sql)
# people_info = cur.fetchall()
# print("The relevant people are: \n", json.dumps(people_info, indent=2))

#t_csv_load()
#t_csv_match()
#t_csv_matchall()
#t_csv_key2tmp()
#t_csv_primkey()
#t_csv_delbytmp()
#t_csv_updbytmp()
#t_csv_updbykey()
#t_csv_insert()
t_rdb_primtmp()
