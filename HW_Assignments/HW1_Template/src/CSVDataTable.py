
from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.


        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._logger = logging.getLogger()

        # NOTE: commented out for now because cannot convert key columns (a dictionary) to json
        #self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):
    # toString method

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            # shows first 5 lines and last 5 lines
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0,CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)

            for r in csv_d_rdr:
                self._add_row(r)

        #print(self._rows[0].get("playerID"))


        dict = {}
        for i in range(0,1):#len(self._rows)):
            r = self._rows[i]
            print(r.keys())
            t = ()
            if self._data["key_columns"] is not None:
                for key in self._data["key_columns"]:
                    if not r[key]:
                        print(r)
                        raise Exception("Null key")
                    t += (r.get(key),)

                if t in dict.keys():
                    print(t)
                    raise Exception("Duplicate key.")

            dict[t] = True

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        tmp = self.key_to_template(key_fields)
        return self.find_by_template(tmp, field_list)

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        result = []
        for r in self._rows:
            if self.matches_template(r,template):
                d = {}
                if field_list is not None:
                    for f in field_list:
                        d[f] = r.get(f)
                else:
                    d = r
                result.append(d)
        return result

    def key_to_template(self,key):
        tmp = {}
        i = 0
        for col in self._data["key_columns"]:
            tmp[col] = key[i]
            i+=1
        return tmp

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        tmp = self.key_to_template(key_fields)
        return self.delete_by_template(tmp)

    def delete_by_template(self, template):
        """
        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """

        full_tmp = self.find_by_template(template,None)
        for r in full_tmp:
            self._rows.remove(r)
        return len(full_tmp)
        #NOTE: maybe also delete from CSV

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        tmp = self.key_to_template(key_fields)
        return self.update_by_template(tmp, new_values)

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        for k_p in self._data["key_columns"]:
            for k_tmp in new_values.keys():
                if k_tmp == k_p and self.find_by_template({k_tmp:new_values[k_tmp]}):
                    print(self.find_by_template({k_tmp:new_values[k_tmp]}))
                    raise Exception("Duplicate key.")
                if k_tmp == k_p and not new_values[k_tmp]:
                    raise Exception("Cannot change primary key to null.")

        count = 0
        for r in self._rows:
            if self.matches_template(r, template):
                # since this works, this means that r is a reference not a copy?
                for f in new_values.keys():
                    r[f] = new_values[f]
                print(r)
                count += 1
        return count

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        for k_p in self._data["key_columns"]:
            found = False
            for k_tmp in new_record.keys():
                if k_tmp == k_p and self.find_by_template({k_tmp:new_record[k_tmp]}):
                    raise Exception("Duplicate key.")
                if k_tmp == k_p:
                    found = True
            if (found is not True):
                raise Exception("Cannot insert new record without primary key.")

        self._rows.append(new_record)
        return None

    def get_rows(self):
        return self._rows

