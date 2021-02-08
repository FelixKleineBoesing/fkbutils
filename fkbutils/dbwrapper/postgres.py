import psycopg2
from psycopg2 import sql
from psycopg2.sql import SQL, Identifier, Placeholder
from psycopg2.extras import execute_batch
import logging
from typing import Union, List
import pandas as pd


class PostgresWrapper:

    def __init__(self, host: str, port: int, user: str, password: str, dbname: str, schema: str = None,
                 connect_args: list = None):
        """

        :param host:
        :param port:
        :param user:
        :param password:
        :param dbname:
        :param schema:
        :param connect_args: additional connection arguments that will be delivered to psycopgs2.connect.
            Schema is one of them
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.schema = schema
        self.connection_string = "dbname='{}' user='{}' host='{}' password='{}' port='{}'". \
            format(dbname, user, host, password, port)

        if connect_args is None:
            connect_args = []
        self.connect_args = connect_args
        self.args = self._get_full_args(connect_args=connect_args, schema=schema)

    @staticmethod
    def _get_full_args(connect_args: list, schema: str = None) -> str:
        """
        construct the additional arguments for the postgres connection

        :param connect_args:
        :param schema:
        :return:
        """
        if schema is not None:
            connect_args.append("-c search_path={}".format(schema))
        args = " ".join(connect_args)
        return args

    @classmethod
    def from_config_manager(cls, config_manager):
        """
        constructs a PostgresWrapper from a config manager by using environment variables

        :param config_manager:
        :return:
        """
        return cls(host=config_manager.get_value("POSTGRES_HOST"),
                   port=config_manager.get_value("POSTGRES_PORT"),
                   user=config_manager.get_value("POSTGRES_USER"),
                   password=config_manager.get_value("POSTGRES_PASSWORD"),
                   dbname=config_manager.get_value("POSTGRES_DB"))

    def get_column_names(self, table_name: str = None, cursor=None, schema: str = None) -> list:
        """
        either supply a table_name or a cursor from a query

        :param table_name: name of the table
        :param cursor: psycopg2 cursor
        :param schema: if you want to query a different schema than the one that is supplied in the constructor, you
            can supply the schema with this argument
        :return:
        """
        assert table_name is not None or cursor is not None
        if table_name is not None:
            cursor = self.get_query("SELECT * from {} LIMIT 1;".format(table_name), schema=schema)

        desc = cursor.description
        column_names = [des[0] for des in desc]
        return column_names

    def get_table(self, table_name: str, columns: List[str] = None, where_condition: List[dict] = None, group: List[str] = None,
                  order_by: List[dict] = None, table_joins: list = None, schema: str = None) -> pd.DataFrame:
        """
        returns a sql table as a pandas dataframe.

        :param table_name: name of the postgres table
        :param columns: list of strings or dictionaries. if group is not None this must be a list of dictionaries
            containing each at least the keys "column" and "func". "func" contains the aggregaction function by which
            the specified column should be aggregated. You are able to specify a new name of this column with the key
            "new_name"
        :param where_condition: where condition for the selected columns (must be a list containing dictionaries)
        :param group: list of columns by which the table should be grouped
        :param order_by: list of dictionaries which describes the columns and the method by which the output should
            be sorted
        :param table_joins: list of dictionaries which describe the tables and the columns which should be joined
            with the original table
        :param schema: This can be used for example if you want to use the same database but another schema.

        :return:
        """
        self._assert_columns(columns, group)
        self._assert_order_by(order_by)

        string, sql_objects = self._get_column_sql_string(columns=columns, group=group)

        string += " FROM {}"
        sql_objects.append(sql.Identifier(table_name))

        where_string, where_objects, where_values = self._get_where_condition(where_condition=where_condition)
        string += where_string
        sql_objects.extend(where_objects)
        values = where_values

        if group is not None:
            string += " GROUP BY {}"
            sql_objects.append(sql.SQL(", ").join(map(sql.Identifier, group)))

        order_string, order_objects = self._get_order_by(order_by=order_by)
        string += order_string
        sql_objects.extend(order_objects)

        if table_joins is not None:
            raise NotImplementedError("This argument is yet not implemented!")

        string += ";"
        sql_string = sql.SQL(string).format(*sql_objects)
        table = self.get_query(query=sql_string, fetch=True, values=values, schema=schema)
        return table

    def get_query(self, query: Union[SQL, str], fetch: bool = True, values: list = None, schema: str = None):
        """
        send a generic query and fetch the results

        :param query: SQL query string
        :param fetch: whether to fetch or not inside this function
        :param values: if placeholder present in query, supply values with this argument
        :param schema: This can be used for example if you want to use the same database but another schema.

        :return: depending on the fetch argument
        """
        args = self._get_full_args(self.connect_args, schema=schema) if schema is not None else self.args
        if isinstance(query, str):
            query = SQL(query)

        with psycopg2.connect(self.connection_string, options=args) as conn:
            logging.debug(query.as_string(conn))
            cursor = conn.cursor()
            if values is None:
                cursor.execute(query.as_string(cursor))
            else:
                cursor.execute(query.as_string(cursor), values)
            if fetch:
                table = cursor.fetchall()
                if len(table) > 0:
                    data = pd.DataFrame(table, columns=self.get_column_names(cursor=cursor))
                else:
                    data = pd.DataFrame(table)
                return data
            else:
                return cursor

    def insert_from_df(self, data: pd.DataFrame, table_name: str, page_size: int = 1000, commit: bool = False,
                       on_conflict: str = None, id_cols: list = None, update_cols: list = None,
                       schema: str = None) -> None:
        """

        :param data: dataframe that should be written to the database
        :param table_name: name of the database table
        :param page_size: chunk size
        :param commit: whether to commit or not
        :param on_conflict: Can be "do_nothing", "do_update
        :param id_cols:
        :param update_cols:
        :param schema: This can be used for example if you want to use the same database but another schema.

        :return:
        """
        assert (isinstance(data, pd.DataFrame))
        assert (isinstance(table_name, str))
        assert (isinstance(page_size, int))
        assert (isinstance(commit, bool))
        assert (on_conflict in ["do_nothing", "do_update", None])
        assert (isinstance(id_cols, (list, type(None))))
        assert (isinstance(update_cols, (list, type(None))))
        assert (isinstance(schema, (type(None), str)))
        args = self._get_full_args(self.connect_args, schema=schema) if schema is not None else self.args

        data.replace(to_replace=[float('nan')], value=[None], inplace=True)
        df_columns = list(data)
        columns = (', '.join('"' + col + '"' for col in df_columns))
        # create VALUES('%s', '%s",...) one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(table_name, columns, values)

        # add "on conflict" statement
        if on_conflict == "do_nothing":
            if id_cols is None:
                insert_stmt = "{} ON CONFLICT DO NOTHING".format(insert_stmt)
            else:
                insert_stmt = "{} ON CONFLICT ({}) DO NOTHING".format(insert_stmt,
                                                                      (', '.join('"' + col + '"' for col in id_cols)))
        elif on_conflict == "do_update":
            if update_cols is None:
                update_cols = columns
            update_stmt = "{} ON CONFLICT({}) DO UPDATE SET ({}) = ({})" if len(update_cols) > 1 \
                else "{} ON CONFLICT({}) DO UPDATE SET {} = {}"
            insert_stmt = update_stmt.format(insert_stmt, (', '.join('"' + col + '"' for col in id_cols)),
                                             (', '.join('"' + col + '"' for col in update_cols)),
                                             (', '.join('EXCLUDED."' + col + '"' for col in update_cols)))

        logging.debug("Upsert rows ...") if on_conflict == "do_update" else logging.debug("Insert rows ...")
        with psycopg2.connect(self.connection_string, options=args) as conn:
            cursor = conn.cursor()
            execute_batch(cursor, insert_stmt, data.values, page_size=page_size)
        if commit:
            conn.commit()
            logging.debug("Data succesfully inserted.")

    @staticmethod
    def _get_column_sql_string(columns: list = None, group: list = None) -> (str, list):
        """
        get columns as a string that can be used for querying

        :param columns:
        :param group:
        :return:
        """
        string = "SELECT"
        if columns is None:
            if group is None:
                string += " * "
                sql_objects = []
            else:
                string += " {} "
                sql_objects = [sql.SQL(",").join(map(sql.Identifier, group))]
        else:
            string = "SELECT {}"
            if group is None:
                cols = sql.SQL(",").join(map(sql.Identifier, columns))
                sql_objects = [cols]
            else:
                sql_objects = [sql.SQL(",").join(map(sql.Identifier, group))]
                for i, col in enumerate(columns):
                    if "new_name" not in col:
                        name = "{}_{}".format(col["func"], col["column"])
                    else:
                        name = col["new_name"]
                    string += ", {}({}) AS {}".format(col["func"], "{}", name)
                    sql_objects.append(sql.Identifier(col["column"]))
        return string, sql_objects

    @staticmethod
    def _assert_columns(columns: list = None, group: list = None) -> None:
        """
        asserts that the columns are all of type string or dictionary

        :param columns:
        :param group:
        :return:
        """
        if columns is not None:
            assert isinstance(columns, list)
            if group is None:
                assert all([isinstance(col, str) for col in columns])
            else:
                assert all([isinstance(col, dict) for col in columns])
                assert all([all([key in agg_col for key in ["column", "func"]]) for agg_col in columns])

    @staticmethod
    def _assert_order_by(order_by: list = None) -> None:
        """
        asserts that the order by dict has the right format

        :param order_by:
        :return:
        """
        if order_by is not None:
            for order in order_by:
                assert isinstance(order, dict)
                assert all([key in ["column", "method"] for key in order.keys()])
                assert all([method.lower() in ["asc", "desc"] for method in [order["method"] for order in order_by]])

    @staticmethod
    def _get_where_condition(where_condition: list = None) -> (str, list, list):
        """
        constructs the where condition

        :param where_condition:
        :return:
        """
        sql_objects = []
        if where_condition is not None:
            string = " WHERE " + " and ".join(["{} {} {}".format("{}", cond["condition"], "{}")
                                               for cond in where_condition])
            for item in where_condition:
                sql_objects.extend([sql.Identifier(item["column"]), sql.Placeholder()])
            values = []
            for cond in where_condition:
                if cond["condition"].lower() == "in":
                    if isinstance(cond["value"], (list, tuple)):
                        cond["value"] = tuple(cond["value"])
                    else:
                        raise ValueError("If condition 'IN' is used, the value must be a list or tuple!")
                values.append(cond["value"])
        else:
            string = ""
            values = None

        return string, sql_objects, values

    @staticmethod
    def _get_order_by(order_by: list = None) -> (str, list):
        """
        construct the order by string and sql objects

        :param order_by:
        :return:
        """
        if order_by is not None:
            order_string = " ORDER BY "
            sql_objects = list(map(sql.Identifier, [order["column"] for order in order_by]))
            order_strings = ["{} {}".format("{}", order["method"]) for order in order_by]
            order_string += ", ".join(order_strings)
        else:
            sql_objects = []
            order_string = ""
        return order_string, sql_objects


if __name__ == "__main__":
    postgres_wrapper = PostgresWrapper(host="localhost", port=5432, user="felix", password="felix", dbname="test")
    dummy_df = pd.DataFrame()
    postgres_wrapper.insert_from_df(data=dummy_df, table_name="Testdata")