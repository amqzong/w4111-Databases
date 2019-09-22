from src.BaseDataTable import BaseDataTable
import pymysql

class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns
        }

        h = connect_info["host"]
        u = connect_info["user"]
        p = connect_info["password"]
        d = connect_info["db"]
        c = connect_info["charset"]

        self._db = pymysql.connect(host = h, user = u, password = p, db = d, charset = c)
        self._dbcur = self._db.cursor()
        self._tbl = table_name

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
        sql = self.create_select(self._tbl,template,field_list)
        print(sql)
        res = self._dbcur.execute(sql)
        return self._dbcur.fetchall()

    def key_to_template(self,key):
        tmp = {}
        i = 0
        if (len(key) < len(self._data["key_columns"])):
            raise Exception("Null key.")
        for col in self._data["key_columns"]:
            if not key[i]:
                raise Exception("Null key.")
            tmp[col] = key[i]
            i+=1
        return tmp

    def template_to_where_clause(self, template):
        """

        :param template: One of those weird templates
        :return: WHERE clause corresponding to the template.
        """

        if template is None or template == {}:
            w_clause = ""
        else:
            args = []
            terms = []

            for k, v in template.items():
                terms.append(" " + k + f"=\'{v}\' ")

            w_clause = "AND".join(terms)
            w_clause = " WHERE " + w_clause
        return w_clause

    def create_select(self, table_name, template, fields, order_by=None, limit=None, offset=None):
        """
        Produce a select statement: sql string and args.

        :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
        :param fields: Columns to select (an array of column name)
        :param template: One of Don Ferguson's weird JSON/python dictionary templates.
        :param order_by: Ignore for now.
        :param limit: Ignore for now.
        :param offset: Ignore for now.
        :return: sql string
        """

        if fields is None:
            field_list = " * "
        else:
            field_list = " " + ",".join(fields) + " "

        w_clause = self.template_to_where_clause(template)
        sql = "select " + field_list + " from " + table_name + " " + w_clause
        return sql

    def create_delete(self, table_name, template):
        w_clause = self.template_to_where_clause(template)
        sql = "delete from " + table_name + " " + w_clause
        return sql

    def create_insert(self, table_name, dict):
        cols = []
        vals = []
        for (k, v) in dict.items():
            cols.append(k)
            vals.append("\'"+v+"\'")

        sql = "insert into " + self._tbl + " (" + ','.join(cols) + ") values (" + ','.join(vals) + ")"
        return sql

    def create_update(self, table_name, template, new_values):
        l = []
        for k, v in new_values.items():
            l.append(" " + k + f"=\'{v}\' ")
        w_clause = self.template_to_where_clause(template)
        sql = "update " + self._tbl + " set " + ','.join(l) + w_clause
        return sql

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
        sql = self.create_delete(self._tbl, template)
        print(sql)
        res = self._dbcur.execute(sql)
        return res

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
        sql = self.create_update(self._tbl, template, new_values)
        print(sql)
        for k in new_values.keys():
            if k in self._data["key_columns"] and new_values[k] is None:
                raise Exception("Null key.")
        res = self._dbcur.execute(sql)
        return res

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        sql = self.create_insert(self._tbl, new_record)
        print(sql)
        res = self._dbcur.execute(sql)
        print(res)

    def get_rows(self):
        return self._rows




