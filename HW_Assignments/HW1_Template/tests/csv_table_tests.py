#Test CSV exceptions

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

# Raise exception when one of primary key columns is null
def t_csv_e1():

    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["G_defense"])

    print("Created table = " + str(csv_tbl))

# Raise exception when primary key is not unique for each row
def t_csv_e2():

    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["yearID"])

    print("Created table = " + str(csv_tbl))

# Should work fine when primary key is unique (but a column in primary key may not be unique)
def t_csv_e3():

    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["yearID", "playerID", "teamID"])


    print("Created table = " + str(csv_tbl))

# When you delete one row that matches template, other rows should be fine
def t_csv_e4():
    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["teamID", "playerID", "yearID"])
    find_tmp = {"teamID": "BS1"}

    print(csv_tbl.find_by_template(find_tmp,field_list=["playerID"]))

    del_tmp = {"playerID": "barnero01"}
    print(csv_tbl.delete_by_template(del_tmp))

    print(csv_tbl.find_by_template(find_tmp,field_list=["playerID"]))

# Raise exception when trying to update row with a set of primary keys that already exists in table
def t_csv_e5():
    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["teamID", "playerID", "yearID"])

    tmp = {"yearID": "1871","teamID":"BS1","playerID": "barnero01"}
    new_values = {"yearID": "1871","teamID":"BS1","playerID": "barrofr01"}
    csv_tbl.update_by_template(tmp,new_values)

# Can update row with a primary key that already exists in table as long as set of primary keys for the row is unique
def t_csv_e6():
    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["teamID", "playerID", "yearID"])

    tmp = {"playerID": "barnero01"}
    new_values = {"playerID": "brannmi01"}
    csv_tbl.update_by_template(tmp,new_values)

# Raise exception when trying to update row with a null primary key
def t_csv_e7():
    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["teamID", "playerID", "yearID"])

    tmp = {"playerID": "barnero01"}
    new_values = {"playerID": ""}
    csv_tbl.update_by_template(tmp,new_values)
    #csv_tbl.update_by_key(["gomezle01"],new_values)

# Raise exception when trying to insert row with a set of primary keys that already exists in table
def t_csv_e8():
    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID","teamID","yearID"])

    new_values = {"yearID": "1871","teamID":"BS1","playerID": "barnero01"}
    csv_tbl.insert(new_values)

# Can insert row with a primary key that already exists in table as long as set of primary keys for the row is unique
def t_csv_e9():
    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["teamID", "playerID", "yearID"])

    new_values = {"yearID": "2017","teamID":"BS1","playerID": "barnero01"}
    csv_tbl.insert(new_values)

# Raise exception when trying to insert row with a primary key that is null
def t_csv_e10():
    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["teamID", "playerID", "yearID"])

    new_values = {"playerID":"","yearID": "2019"}
    csv_tbl.insert(new_values)

# If template is empty, treat it like no where clause (so return all)
# If field list is empty, return all fields
def t_csv_e11():
    tmp = {}
    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["teamID", "playerID", "yearID"])

    result = csv_tbl.find_by_template(tmp)
    print(json.dumps(result, indent=2))

#Error if pass in empty template for update_by_key
def t_csv_e12():
    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["teamID", "playerID", "yearID"])

    tmp = {}
    new_values = {"yearID":"2017","playerID":"amz2136","G_batting":"1"}
    print(csv_tbl.update_by_key(tmp,new_values))

#Error if pass in empty template for delete_by_key
def t_csv_e13():
    connect_info = {
        "directory": data_dir,
        "file_name": "Appearances.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["teamID", "playerID", "yearID"])

    tmp = {}
    print(csv_tbl.delete_by_key(tmp))

#t_csv_e1()
#t_csv_e2()
#t_csv_e3()
#t_csv_e4()
#t_csv_e5()
#t_csv_e6()
#t_csv_e7()
#t_csv_e8()
#t_csv_e9()
#t_csv_e10()
#t_csv_e11()
#t_csv_e12()
t_csv_e13()