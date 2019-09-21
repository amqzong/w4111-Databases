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

    csv_tbl = CSVDataTable("people", connect_info, key_columns=None)

    print("Created table = " + str(csv_tbl))

def t_csv_match():
    row = {"cool":"yes","db":"no"}
    t = {"cool":"yes"}
    result = CSVDataTable.matches_template(row,t)
    print(result)

def t_csv_findbytmp():
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

#Test CSV exceptions

# Raise exception when one of primary key columns is null
def t_csv_e1():

    connect_info = {
        "directory": data_dir,
        "file_name": "AllstarFull_AZ.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["startingPos"])

    print("Created table = " + str(csv_tbl))

# Raise exception when primary key is not unique for each row
def t_csv_e2():

    connect_info = {
        "directory": data_dir,
        "file_name": "AllstarFull_AZ.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["yearID"])

    print("Created table = " + str(csv_tbl))

# Should work fine when primary key is unique (but a column in primary key may not be unique)
def t_csv_e3():

    connect_info = {
        "directory": data_dir,
        "file_name": "AllstarFull_AZ.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["yearID", "playerID"])


    print("Created table = " + str(csv_tbl))

# When you delete one row that matches template, other rows should be fine
def t_csv_e4():
    connect_info = {
        "directory": data_dir,
        "file_name": "AllstarFull_AZ.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["teamID", "playerID"])
    find_tmp = {"teamID": "NYA"}

    print(csv_tbl.find_by_template(find_tmp))

    del_tmp = {"playerID": "gomezle01"}
    print(csv_tbl.delete_by_template(del_tmp))

    print(csv_tbl.find_by_template(find_tmp))

# Raise exception when trying to update row with a primary key that already exists in table
def t_csv_e5():
    connect_info = {
        "directory": data_dir,
        "file_name": "AllstarFull_AZ.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID"])

    tmp = {"playerID": "gomezle01"}
    new_values = {"playerID": "ferreri01"}
    #csv_tbl.update_by_template(tmp,new_values)
    csv_tbl.update_by_key(["gomezle01"],new_values)

# Raise exception when trying to update row with a null primary key
def t_csv_e6():
    connect_info = {
        "directory": data_dir,
        "file_name": "AllstarFull_AZ.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID"])

    tmp = {"playerID": "gomezle01"}
    new_values = {"playerID": ""}
    csv_tbl.update_by_template(tmp,new_values)
    #csv_tbl.update_by_key(["gomezle01"],new_values)

# Raise exception when trying to insert row with a primary key that already exists in table
def t_csv_e7():
    connect_info = {
        "directory": data_dir,
        "file_name": "AllstarFull.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID"])

    new_values = {"playerID":"gomezle01","yearID": "2019"}
    csv_tbl.insert(new_values)

# Raise exception when trying to insert row with a primary key that is null
def t_csv_e8():
    connect_info = {
        "directory": data_dir,
        "file_name": "AllstarFull.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["teamID", "playerID"])

    new_values = {"playerID":"","yearID": "2019"}
    csv_tbl.insert(new_values)

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

def t_rdb_findbytmp():
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

def t_rdb_findbykey():
    connect_info = {
        "host": "localhost",
        "user": "root",
        "password": "Databases",
        "db": "w4111",
        "charset": "utf8mb4"
    }
    # db = pymysql.connect(host="localhost", user="root", password="Databases", db="w4111", charset="utf8mb4")
    # dbcur = db.cursor()
    # sql = "SHOW KEYS FROM people WHERE Key_name = 'PRIMARY'"
    # dbcur.execute(sql)
    # print(dbcur.fetchall())

    k = ["willite01"]
    fields = {
        "playerID", "nameLast", "nameFirst"
    }
    rdb_tbl = RDBDataTable("people", connect_info, key_columns=["playerID"])
    print(rdb_tbl.find_by_primary_key(k,fields))

def t_rdb_delbytmp():
    connect_info = {
        "host": "localhost",
        "user": "root",
        "password": "Databases",
        "db": "w4111",
        "charset": "utf8mb4"
    }

    rdb_tbl = RDBDataTable("people", connect_info, None)

    tmp = {
        "nameFirst": "Ted",
        "nameLast": "Williams"
    }

    print(rdb_tbl.delete_by_template(tmp))

def t_rdb_delbykey():
    connect_info = {
        "host": "localhost",
        "user": "root",
        "password": "Databases",
        "db": "w4111",
        "charset": "utf8mb4"
    }

    k = ["willite01"]

    rdb_tbl = RDBDataTable("people", connect_info, key_columns=["playerID"])
    print(rdb_tbl.delete_by_key(k))

def t_rdb_insert():
    connect_info = {
        "host": "localhost",
        "user": "root",
        "password": "Databases",
        "db": "w4111",
        "charset": "utf8mb4"
    }

    rdb_tbl = RDBDataTable("people", connect_info, None)

    tmp = {
        "nameFirst": "Amanda",
        "nameLast": "Zong"
    }

    rdb_tbl.insert(tmp)

def t_rdb_updbytmp():

    connect_info = {
        "host": "localhost",
        "user": "root",
        "password": "Databases",
        "db": "w4111",
        "charset": "utf8mb4"
    }

    rdb_tbl = RDBDataTable("people", connect_info, None)

    tmp = {
        "nameFirst": "Amanda",
        "nameLast": "Zong"
    }

    new_values = {
        "playerID": "amz2136",
        "birthYear": "1999"
    }
    rdb_tbl.insert(tmp)
    print(rdb_tbl.update_by_template(tmp, new_values))

def t_rdb_updbykey():

    connect_info = {
        "host": "localhost",
        "user": "root",
        "password": "Databases",
        "db": "w4111",
        "charset": "utf8mb4"
    }

    rdb_tbl = RDBDataTable("people", connect_info, key_columns=["playerID"])

    k = ["willite01"]

    new_values = {
        "nameLast": "Zong",
        "birthYear": "1999"
    }
    print(rdb_tbl.update_by_key(k, new_values))

#t_csv_load()
#t_csv_match()
#t_csv_findbytmp()
#t_csv_key2tmp()
#t_csv_primkey()
#t_csv_delbytmp()
#t_csv_updbytmp()
#t_csv_updbykey()
#t_csv_insert()

#t_csv_e1()
#t_csv_e2()
#t_csv_e3()
#t_csv_e4()
#t_csv_e5()
#t_csv_e6()
#t_csv_e7()
#t_csv_e8()

#t_rdb_findbytmp()
#t_rdb_findbykey()
#t_rdb_delbytmp()
#t_rdb_delbykey()
#t_rdb_insert()
#t_rdb_updbytmp()
#t_rdb_updbykey()