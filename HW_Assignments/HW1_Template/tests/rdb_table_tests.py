# Raise exception when trying to update row with a primary key that already exists in table

from src.RDBDataTable import RDBDataTable
import logging
import os
import json
import pymysql

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")

# Can change one of primary keys to be the same as existing as long as the set of primary keys for a row is unique
def t_rdb_e1():
    connect_info = {
        "host": "localhost",
        "user": "root",
        "password": "Databases",
        "db": "w4111",
        "charset": "utf8mb4"
    }

    rdb_tbl = RDBDataTable("appearances", connect_info, key_columns=["playerID","teamID","yearID"])

    tmp = {"yearID": "1871"}
    new_values = {"yearID": "2019"}
    print(rdb_tbl.find_by_template(tmp,field_list={"playerID","yearID"}))
    rdb_tbl.update_by_template(tmp,new_values)
    tmp = {"yearID": "1871"}
    print(rdb_tbl.find_by_template(tmp, field_list={"playerID","yearID"}))
    tmp = {"yearID": "2019"}
    print(rdb_tbl.find_by_template(tmp, field_list={"playerID", "yearID"}))

# Cannot update the set of primary keys for a row to be the same as an existing row
def t_rdb_e2():
    connect_info = {
        "host": "localhost",
        "user": "root",
        "password": "Databases",
        "db": "w4111",
        "charset": "utf8mb4"
    }

    rdb_tbl = RDBDataTable("appearances", connect_info, key_columns=["playerID","teamID","yearID"])

    tmp = {"yearID": "1871","playerID": "abercda01","teamID":"TRO"}
    new_values = {"yearID": "1871","playerID": "addybo01","teamID":"RC1"}
    print(rdb_tbl.find_by_template(tmp,field_list={"playerID","yearID"}))
    rdb_tbl.update_by_template(tmp,new_values)

# Cannot update a primary key for a row to be null
def t_rdb_e3():
    connect_info = {
        "host": "localhost",
        "user": "root",
        "password": "Databases",
        "db": "w4111",
        "charset": "utf8mb4"
    }

    rdb_tbl = RDBDataTable("appearances", connect_info, key_columns=["playerID","teamID","yearID"])

    tmp = {"yearID": "1871","playerID": "abercda01","teamID":"TRO"}
    print(rdb_tbl.find_by_template(tmp))
    new_values = {"yearID": None}
    rdb_tbl.update_by_template(tmp,new_values)
    tmp = {"playerID": "abercda01","teamID":"TRO"}
    print(rdb_tbl.find_by_template(tmp))

# Cannot insert without all primary keys defined
def t_rdb_e4():
    connect_info = {
        "host": "localhost",
        "user": "root",
        "password": "Databases",
        "db": "w4111",
        "charset": "utf8mb4"
    }

    rdb_tbl = RDBDataTable("appearances", connect_info, key_columns=["playerID","teamID","yearID"])

    tmp = {"yearID":None,"playerID":"amz2136","G_batting":"1"}
    print(rdb_tbl.insert(tmp))

# Cannot insert a row with a set of primary keys the same as an existing row
def t_rdb_e5():
    connect_info = {
        "host": "localhost",
        "user": "root",
        "password": "Databases",
        "db": "w4111",
        "charset": "utf8mb4"
    }

    rdb_tbl = RDBDataTable("appearances", connect_info, key_columns=["playerID","teamID","yearID"])

    tmp = {"yearID": "1871","playerID": "abercda01","teamID":"TRO"}
    print(rdb_tbl.insert(tmp))

# If template is empty, treat it like no where clause (so return all)
# If field list is empty, return all fields
def t_rdb_e6():
    connect_info = {
        "host": "localhost",
        "user": "root",
        "password": "Databases",
        "db": "w4111",
        "charset": "utf8mb4"
    }

    rdb_tbl = RDBDataTable("appearances", connect_info, key_columns=["playerID","teamID","yearID"])

    tmp = {}
    print(rdb_tbl.find_by_template(tmp))

#Error if pass in empty template for update_by_key
def t_rdb_e7():
    connect_info = {
        "host": "localhost",
        "user": "root",
        "password": "Databases",
        "db": "w4111",
        "charset": "utf8mb4"
    }

    rdb_tbl = RDBDataTable("appearances", connect_info, key_columns=["playerID","teamID","yearID"])

    tmp = {}
    new_values = {"yearID":"2017","playerID":"amz2136","G_batting":"1"}
    print(rdb_tbl.update_by_key(tmp,new_values))

#Error if pass in empty template for delete_by_key
def t_rdb_e8():
    connect_info = {
        "host": "localhost",
        "user": "root",
        "password": "Databases",
        "db": "w4111",
        "charset": "utf8mb4"
    }

    rdb_tbl = RDBDataTable("appearances", connect_info, key_columns=["playerID","teamID","yearID"])

    tmp = {}
    print(rdb_tbl.delete_by_key(tmp))

#t_rdb_e1()
#t_rdb_e2()
#t_rdb_e3()
#t_rdb_e4()
#t_rdb_e5()
#t_rdb_e6()
#t_rdb_e7()
t_rdb_e8()